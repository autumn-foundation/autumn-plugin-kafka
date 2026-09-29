//! The default backend. It uses `rdkafka` (`librdkafka`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

use crate::backend::{Backend, ConsumerBackend, ConsumerSpec, ProducerBackend};
use crate::config::KafkaConfig;
use crate::error::KafkaError;

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
pub(crate) fn producer_properties(_config: &KafkaConfig) -> BTreeMap<String, String> {
    todo!()
}

/// Returns the `librdkafka` properties for one consumer.
pub(crate) fn consumer_properties(
    _config: &KafkaConfig,
    _spec: &ConsumerSpec,
) -> BTreeMap<String, String> {
    todo!()
}

impl Backend for RdKafkaBackend {
    fn producer(&self, _config: &KafkaConfig) -> Result<Arc<dyn ProducerBackend>, KafkaError> {
        todo!()
    }

    fn consumer(
        &self,
        _config: &KafkaConfig,
        _spec: &ConsumerSpec,
    ) -> Result<Box<dyn ConsumerBackend>, KafkaError> {
        todo!()
    }
}

#[allow(dead_code)]
fn unused(_: Duration, _: BoxFuture<'_, ()>) {}

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
