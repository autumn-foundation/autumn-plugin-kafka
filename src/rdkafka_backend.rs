//! The default backend. It uses `rdkafka` (`librdkafka`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::error::KafkaError as RdKafkaError;
use rdkafka::message::{BorrowedMessage, Header, Headers as _, Message as _, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as _};
use rdkafka::util::Timeout;

use crate::backend::{Backend, ConsumerBackend, ConsumerSpec, ProducerBackend};
use crate::config::{ClientRole, KafkaConfig};
use crate::error::KafkaError;
use crate::message::{Delivery, Message, Record};

/// The default backend. It uses `librdkafka`.
///
/// The producer sets `enable.idempotence = true` and
/// `message.timeout.ms = producer.send_timeout_ms`, if the config does not set them.
/// Consumers always use `enable.auto.offset.store = false` and
/// `enable.auto.commit = true`. The plugin stores an offset only after the
/// handler completes. Thus delivery is at-least-once.
#[derive(Debug, Clone, Copy, Default)]
pub struct RdKafkaBackend;

/// Returns the `librdkafka` properties for the producer.
pub fn producer_properties(config: &KafkaConfig) -> BTreeMap<String, String> {
    let mut props = BTreeMap::from([
        ("enable.idempotence".to_owned(), "true".to_owned()),
        (
            "message.timeout.ms".to_owned(),
            config.producer.send_timeout_ms.to_string(),
        ),
    ]);
    props.extend(config.client_properties(ClientRole::Producer));
    props
}

/// Returns the `librdkafka` properties for one consumer.
pub fn consumer_properties(config: &KafkaConfig, spec: &ConsumerSpec) -> BTreeMap<String, String> {
    let mut props = config.client_properties(ClientRole::Consumer);
    props.extend(spec.properties.clone());
    for (key, value) in [
        ("group.id", spec.group_id.as_str()),
        ("enable.auto.offset.store", "false"),
        ("enable.auto.commit", "true"),
    ] {
        props.insert(key.to_owned(), value.to_owned());
    }
    props
}

fn client_config(props: &BTreeMap<String, String>) -> ClientConfig {
    let mut client = ClientConfig::new();
    for (key, value) in props {
        client.set(key, value);
    }
    client
}

/// Maps a client error. A config error does not show the value, because it can be a secret.
fn map_error(error: &RdKafkaError) -> KafkaError {
    match error {
        RdKafkaError::ClientConfig(_, description, key, _value) => {
            KafkaError::Config(format!("{description} (property {key:?})"))
        }
        other => KafkaError::Client(other.to_string()),
    }
}

fn join_error(error: &tokio::task::JoinError) -> KafkaError {
    KafkaError::Client(format!("blocking task failed: {error}"))
}

impl Backend for RdKafkaBackend {
    fn producer(&self, config: &KafkaConfig) -> Result<Arc<dyn ProducerBackend>, KafkaError> {
        let producer: FutureProducer = client_config(&producer_properties(config))
            .create()
            .map_err(|e| map_error(&e))?;
        Ok(Arc::new(RdProducer { producer }))
    }

    fn consumer(
        &self,
        config: &KafkaConfig,
        spec: &ConsumerSpec,
    ) -> Result<Box<dyn ConsumerBackend>, KafkaError> {
        let consumer: StreamConsumer = client_config(&consumer_properties(config, spec))
            .create()
            .map_err(|e| map_error(&e))?;
        let topics: Vec<&str> = spec.topics.iter().map(String::as_str).collect();
        consumer.subscribe(&topics).map_err(|e| map_error(&e))?;
        Ok(Box::new(RdConsumer { consumer }))
    }
}

struct RdProducer {
    producer: FutureProducer,
}

impl ProducerBackend for RdProducer {
    fn send(
        &self,
        record: Record,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<Delivery, KafkaError>> {
        Box::pin(async move {
            let mut headers = OwnedHeaders::new_with_capacity(record.headers().len());
            for (key, value) in record.headers() {
                headers = headers.insert(Header {
                    key,
                    value: Some(value),
                });
            }
            let mut future_record: FutureRecord<'_, [u8], [u8]> =
                FutureRecord::to(record.topic()).headers(headers);
            if let Some(key) = record.key() {
                future_record = future_record.key(key);
            }
            if !record.is_tombstone() {
                future_record = future_record.payload(record.payload());
            }
            self.producer
                .send(future_record, Timeout::After(timeout))
                .await
                .map(|d| Delivery {
                    partition: d.partition,
                    offset: d.offset,
                })
                .map_err(|(e, _)| map_error(&e))
        })
    }

    fn ping(&self, timeout: Duration) -> BoxFuture<'_, Result<(), KafkaError>> {
        let producer = self.producer.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                producer
                    .client()
                    .fetch_metadata(None, timeout)
                    .map(|_| ())
                    .map_err(|e| map_error(&e))
            })
            .await
            .map_err(|e| join_error(&e))?
        })
    }

    fn flush(&self, timeout: Duration) -> BoxFuture<'_, Result<(), KafkaError>> {
        let producer = self.producer.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || producer.flush(timeout).map_err(|e| map_error(&e)))
                .await
                .map_err(|e| join_error(&e))?
        })
    }
}

struct RdConsumer {
    consumer: StreamConsumer,
}

fn to_message(m: &BorrowedMessage<'_>) -> Message {
    let mut msg = m
        .payload()
        .map_or_else(
            || Message::tombstone(m.topic()),
            |payload| Message::new(m.topic(), payload),
        )
        .with_position(m.partition(), m.offset());
    if let Some(key) = m.key() {
        msg = msg.with_key(key);
    }
    if let Some(ts) = m.timestamp().to_millis() {
        msg = msg.with_timestamp_ms(ts);
    }
    if let Some(headers) = m.headers() {
        for header in headers.iter() {
            msg = msg.with_header(header.key, header.value.unwrap_or_default());
        }
    }
    msg
}

impl ConsumerBackend for RdConsumer {
    fn recv(&mut self) -> BoxFuture<'_, Result<Message, KafkaError>> {
        Box::pin(async move {
            let borrowed = self.consumer.recv().await.map_err(|e| map_error(&e))?;
            Ok(to_message(&borrowed))
        })
    }

    fn commit(&mut self, message: &Message) -> Result<(), KafkaError> {
        // `store_offset` stores `offset + 1`, the next offset to read.
        self.consumer
            .store_offset(message.topic(), message.partition(), message.offset())
            .map_err(|e| map_error(&e))
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            // The drop leaves the group and commits stored offsets. It blocks.
            let consumer = self.consumer;
            if let Err(error) = tokio::task::spawn_blocking(move || drop(consumer)).await {
                tracing::error!(%error, "Kafka consumer close failed");
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ConsumerSpec {
        ConsumerSpec {
            name: "c".into(),
            group_id: "g".into(),
            topics: vec!["t".into()],
            properties: BTreeMap::from([("fetch.min.bytes".into(), "10".into())]),
        }
    }

    #[test]
    fn producer_properties_have_safe_defaults() {
        let mut config = KafkaConfig::new("k:1");
        config.producer.send_timeout_ms = 1234;

        let props = producer_properties(&config);

        assert_eq!(props["bootstrap.servers"], "k:1");
        assert_eq!(props["enable.idempotence"], "true");
        assert_eq!(props["message.timeout.ms"], "1234");
    }

    #[test]
    fn producer_properties_keep_user_values() {
        let mut config = KafkaConfig::default();
        config
            .properties
            .insert("enable.idempotence".into(), "false".into());
        config
            .producer
            .properties
            .insert("message.timeout.ms".into(), "99".into());

        let props = producer_properties(&config);

        assert_eq!(props["enable.idempotence"], "false");
        assert_eq!(props["message.timeout.ms"], "99");
    }

    #[test]
    fn consumer_properties_force_at_least_once() {
        let mut config = KafkaConfig::default();
        config
            .consumer
            .properties
            .insert("enable.auto.offset.store".into(), "true".into());
        config
            .consumer
            .properties
            .insert("enable.auto.commit".into(), "false".into());
        config
            .consumer
            .properties
            .insert("group.id".into(), "wrong".into());
        config
            .consumer
            .properties
            .insert("fetch.min.bytes".into(), "1".into());

        let props = consumer_properties(&config, &spec());

        assert_eq!(props["group.id"], "g");
        assert_eq!(props["enable.auto.offset.store"], "false");
        assert_eq!(props["enable.auto.commit"], "true");
        assert_eq!(props["fetch.min.bytes"], "10");
    }

    #[tokio::test]
    async fn producer_is_lazy_and_ping_fails_with_no_broker() {
        let config = KafkaConfig::new("127.0.0.1:1");
        let producer = RdKafkaBackend.producer(&config).unwrap();

        let result = producer.ping(Duration::from_millis(300)).await;

        assert!(result.is_err());
    }

    #[test]
    fn unknown_property_is_a_config_error() {
        let mut config = KafkaConfig::default();
        config
            .properties
            .insert("not.a.property".into(), "x".into());

        let producer_err = RdKafkaBackend.producer(&config).err().unwrap();
        let consumer_err = RdKafkaBackend.consumer(&config, &spec()).err().unwrap();

        assert!(
            matches!(producer_err, KafkaError::Config(_)),
            "{producer_err}"
        );
        assert!(
            matches!(consumer_err, KafkaError::Config(_)),
            "{consumer_err}"
        );
    }
}
