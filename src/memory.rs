//! An in-memory broker for tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use tokio::sync::Notify;

use crate::backend::{Backend, ConsumerBackend, ConsumerSpec, ProducerBackend};
use crate::config::KafkaConfig;
use crate::error::KafkaError;
use crate::message::{Delivery, Message, Record};

/// An in-memory broker. Use it in tests. It does not need Docker.
///
/// Each topic has one partition (0).
/// A new group starts at the first message.
/// All consumers in a group get all messages. There is no partition assignment.
/// Clones share the same data.
#[derive(Clone, Default)]
pub struct MemoryBroker {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    state: Mutex<State>,
    notify: Notify,
}

#[derive(Default)]
struct State {
    topics: HashMap<String, Vec<Message>>,
    /// The next offset to read, for each (group, topic).
    committed: HashMap<(String, String), i64>,
    unavailable: bool,
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic cannot leave `State` half-changed, so a poisoned lock is safe to use.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl MemoryBroker {
    /// Makes an empty broker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a record to its topic. Returns its position.
    #[allow(clippy::must_use_candidate)] // Tests often ignore the position.
    #[allow(clippy::missing_panics_doc)] // The offset cannot exceed `i64::MAX`.
    pub fn publish(&self, record: Record) -> Delivery {
        let mut state = self.inner.lock();
        let log = state.topics.entry(record.topic().to_owned()).or_default();
        let offset = i64::try_from(log.len()).expect("offset fits in i64");
        log.push(Message::from_record(record, 0, offset, Some(now_ms())));
        drop(state);
        self.inner.notify.notify_waiters();
        Delivery {
            partition: 0,
            offset,
        }
    }

    /// Returns all messages in a topic.
    #[must_use]
    pub fn messages(&self, topic: &str) -> Vec<Message> {
        self.inner
            .lock()
            .topics
            .get(topic)
            .cloned()
            .unwrap_or_default()
    }

    /// Returns the next offset that the group will read in the topic.
    ///
    /// Returns `None` if the group did not commit in the topic.
    #[must_use]
    pub fn committed_offset(&self, group_id: &str, topic: &str) -> Option<i64> {
        self.inner
            .lock()
            .committed
            .get(&(group_id.to_owned(), topic.to_owned()))
            .copied()
    }

    /// Simulates an outage. When not available, `send` and `ping` fail.
    pub fn set_available(&self, available: bool) {
        self.inner.lock().unavailable = !available;
    }

    /// Makes the broker reject all sends to `topic` with [`KafkaError::Rejected`].
    pub fn reject_topic(&self, _topic: impl Into<String>) {}

    fn check_available(&self) -> Result<(), KafkaError> {
        if self.inner.lock().unavailable {
            return Err(KafkaError::Unavailable("memory broker is down".to_owned()));
        }
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

impl std::fmt::Debug for MemoryBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBroker").finish_non_exhaustive()
    }
}

impl Backend for MemoryBroker {
    fn producer(&self, _config: &KafkaConfig) -> Result<Arc<dyn ProducerBackend>, KafkaError> {
        Ok(Arc::new(self.clone()))
    }

    fn consumer(
        &self,
        _config: &KafkaConfig,
        spec: &ConsumerSpec,
    ) -> Result<Box<dyn ConsumerBackend>, KafkaError> {
        let state = self.inner.lock();
        let positions = spec
            .topics
            .iter()
            .map(|topic| {
                let key = (spec.group_id.clone(), topic.clone());
                (
                    topic.clone(),
                    state.committed.get(&key).copied().unwrap_or(0),
                )
            })
            .collect();
        Ok(Box::new(MemoryConsumer {
            broker: self.clone(),
            group_id: spec.group_id.clone(),
            positions,
        }))
    }
}

impl ProducerBackend for MemoryBroker {
    fn send(
        &self,
        record: Record,
        _timeout: Duration,
    ) -> BoxFuture<'_, Result<Delivery, KafkaError>> {
        let result = self.check_available().map(|()| self.publish(record));
        Box::pin(async move { result })
    }

    fn ping(&self, _timeout: Duration) -> BoxFuture<'_, Result<(), KafkaError>> {
        let result = self.check_available();
        Box::pin(async move { result })
    }

    fn flush(&self, _timeout: Duration) -> BoxFuture<'_, Result<(), KafkaError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A consumer of a [`MemoryBroker`].
struct MemoryConsumer {
    broker: MemoryBroker,
    group_id: String,
    /// The next offset to read, for each topic. The order is the subscription order.
    positions: Vec<(String, i64)>,
}

impl MemoryConsumer {
    fn try_next(&mut self) -> Option<Message> {
        let state = self.broker.inner.lock();
        for (topic, position) in &mut self.positions {
            let index = usize::try_from(*position).unwrap_or(usize::MAX);
            if let Some(msg) = state
                .topics
                .get(topic.as_str())
                .and_then(|log| log.get(index))
            {
                *position += 1;
                return Some(msg.clone());
            }
        }
        None
    }
}

impl ConsumerBackend for MemoryConsumer {
    fn recv(&mut self) -> BoxFuture<'_, Result<Message, KafkaError>> {
        let inner = Arc::clone(&self.broker.inner);
        Box::pin(async move {
            loop {
                let notified = inner.notify.notified();
                tokio::pin!(notified);
                // Register before the check, so that no publish is missed.
                notified.as_mut().enable();
                if let Some(msg) = self.try_next() {
                    return Ok(msg);
                }
                notified.await;
            }
        })
    }

    fn commit(&mut self, message: &Message) -> Result<(), KafkaError> {
        let key = (self.group_id.clone(), message.topic().to_owned());
        self.broker
            .inner
            .lock()
            .committed
            .insert(key, message.offset() + 1);
        Ok(())
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    const WAIT: Duration = Duration::from_secs(1);

    fn spec(group: &str, topics: &[&str]) -> ConsumerSpec {
        ConsumerSpec {
            name: group.to_owned(),
            group_id: group.to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
            properties: BTreeMap::new(),
        }
    }

    fn consumer(broker: &MemoryBroker, group: &str, topics: &[&str]) -> Box<dyn ConsumerBackend> {
        broker
            .consumer(&KafkaConfig::default(), &spec(group, topics))
            .unwrap()
    }

    async fn recv(consumer: &mut Box<dyn ConsumerBackend>) -> Message {
        tokio::time::timeout(WAIT, consumer.recv())
            .await
            .expect("a message in time")
            .unwrap()
    }

    #[tokio::test]
    async fn producer_send_appends_with_increasing_offsets() {
        let broker = MemoryBroker::new();
        let producer = broker.producer(&KafkaConfig::default()).unwrap();

        let first = producer.send(Record::new("t", "a"), WAIT).await.unwrap();
        let second = producer.send(Record::new("t", "b"), WAIT).await.unwrap();

        assert_eq!(
            first,
            Delivery {
                partition: 0,
                offset: 0
            }
        );
        assert_eq!(
            second,
            Delivery {
                partition: 0,
                offset: 1
            }
        );
        let stored = broker.messages("t");
        assert_eq!(stored.len(), 2);
        assert_eq!(stored[1].payload(), b"b");
        assert_eq!(stored[1].offset(), 1);
    }

    #[tokio::test]
    async fn publish_keeps_key_headers_and_tombstones() {
        let broker = MemoryBroker::new();
        broker.publish(Record::new("t", "p").with_key("k").with_header("h", "v"));
        broker.publish(Record::tombstone("t", "k"));

        let stored = broker.messages("t");

        assert_eq!(stored[0].key(), Some(&b"k"[..]));
        assert_eq!(stored[0].header("h"), Some(&b"v"[..]));
        assert!(stored[0].timestamp_ms().is_some());
        assert!(stored[1].is_tombstone());
    }

    #[tokio::test]
    async fn consumer_reads_only_its_topics_in_order() {
        let broker = MemoryBroker::new();
        broker.publish(Record::new("a", "1"));
        broker.publish(Record::new("other", "x"));
        broker.publish(Record::new("a", "2"));
        let mut c = consumer(&broker, "g", &["a"]);

        assert_eq!(recv(&mut c).await.payload(), b"1");
        assert_eq!(recv(&mut c).await.payload(), b"2");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), c.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn consumer_reads_many_topics() {
        let broker = MemoryBroker::new();
        broker.publish(Record::new("a", "1"));
        broker.publish(Record::new("b", "2"));
        let mut c = consumer(&broker, "g", &["a", "b"]);

        let mut got = vec![recv(&mut c).await.topic().to_owned()];
        got.push(recv(&mut c).await.topic().to_owned());
        got.sort();

        assert_eq!(got, ["a", "b"]);
    }

    #[tokio::test]
    async fn recv_waits_for_a_new_message() {
        let broker = MemoryBroker::new();
        let mut c = consumer(&broker, "g", &["t"]);
        let publisher = broker.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            publisher.publish(Record::new("t", "late"));
        });

        assert_eq!(recv(&mut c).await.payload(), b"late");
    }

    #[tokio::test]
    async fn new_consumer_resumes_after_commit() {
        let broker = MemoryBroker::new();
        for p in ["0", "1", "2"] {
            broker.publish(Record::new("t", p));
        }
        let mut c = consumer(&broker, "g", &["t"]);
        let first = recv(&mut c).await;
        c.commit(&first).unwrap();
        let _second = recv(&mut c).await; // not committed
        c.close().await;

        assert_eq!(broker.committed_offset("g", "t"), Some(1));
        let mut again = consumer(&broker, "g", &["t"]);
        assert_eq!(recv(&mut again).await.payload(), b"1");
    }

    #[tokio::test]
    async fn groups_are_independent() {
        let broker = MemoryBroker::new();
        broker.publish(Record::new("t", "x"));
        let mut g1 = consumer(&broker, "g1", &["t"]);
        let msg = recv(&mut g1).await;
        g1.commit(&msg).unwrap();

        let mut g2 = consumer(&broker, "g2", &["t"]);

        assert_eq!(recv(&mut g2).await.payload(), b"x");
        assert_eq!(broker.committed_offset("g2", "t"), None);
    }

    #[tokio::test]
    async fn rejected_topic_fails_send_with_rejected() {
        let broker = MemoryBroker::new();
        let producer = broker.producer(&KafkaConfig::default()).unwrap();
        broker.reject_topic("bad");

        let err = producer
            .send(Record::new("bad", "x"), WAIT)
            .await
            .unwrap_err();

        assert!(matches!(err, KafkaError::Rejected(_)), "{err}");
        assert!(broker.messages("bad").is_empty());
        producer.send(Record::new("good", "x"), WAIT).await.unwrap();
    }

    #[tokio::test]
    async fn outage_fails_send_and_ping() {
        let broker = MemoryBroker::new();
        let producer = broker.producer(&KafkaConfig::default()).unwrap();
        producer.ping(WAIT).await.unwrap();

        broker.set_available(false);

        let err = producer
            .send(Record::new("t", "x"), WAIT)
            .await
            .unwrap_err();
        assert!(matches!(err, KafkaError::Unavailable(_)), "{err}");
        assert!(producer.ping(WAIT).await.is_err());
        assert!(broker.messages("t").is_empty());

        broker.set_available(true);
        producer.send(Record::new("t", "x"), WAIT).await.unwrap();
        producer.flush(WAIT).await.unwrap();
    }
}
