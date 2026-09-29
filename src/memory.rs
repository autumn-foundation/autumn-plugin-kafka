//! An in-memory broker for tests.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

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
    _inner: Arc<()>,
}

impl MemoryBroker {
    /// Makes an empty broker.
    #[must_use]
    pub fn new() -> Self {
        todo!()
    }

    /// Adds a record to its topic. Returns its position.
    pub fn publish(&self, _record: Record) -> Delivery {
        todo!()
    }

    /// Returns all messages in a topic.
    #[must_use]
    pub fn messages(&self, _topic: &str) -> Vec<Message> {
        todo!()
    }

    /// Returns the next offset that the group will read in the topic.
    ///
    /// Returns `None` if the group did not commit in the topic.
    #[must_use]
    pub fn committed_offset(&self, _group_id: &str, _topic: &str) -> Option<i64> {
        todo!()
    }

    /// Simulates an outage. When not available, `send` and `ping` fail.
    pub fn set_available(&self, _available: bool) {
        todo!()
    }
}

impl std::fmt::Debug for MemoryBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBroker").finish_non_exhaustive()
    }
}

impl Backend for MemoryBroker {
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

        assert_eq!(first, Delivery { partition: 0, offset: 0 });
        assert_eq!(second, Delivery { partition: 0, offset: 1 });
        let stored = broker.messages("t");
        assert_eq!(stored.len(), 2);
        assert_eq!(stored[1].payload(), b"b");
        assert_eq!(stored[1].offset(), 1);
    }

    #[tokio::test]
    async fn publish_keeps_key_headers_and_tombstones() {
        let broker = MemoryBroker::new();
        broker.publish(Record::new("t", "p").key("k").header("h", "v"));
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
    async fn outage_fails_send_and_ping() {
        let broker = MemoryBroker::new();
        let producer = broker.producer(&KafkaConfig::default()).unwrap();
        producer.ping(WAIT).await.unwrap();

        broker.set_available(false);

        let err = producer.send(Record::new("t", "x"), WAIT).await.unwrap_err();
        assert!(matches!(err, KafkaError::Unavailable(_)), "{err}");
        assert!(producer.ping(WAIT).await.is_err());
        assert!(broker.messages("t").is_empty());

        broker.set_available(true);
        producer.send(Record::new("t", "x"), WAIT).await.unwrap();
        producer.flush(WAIT).await.unwrap();
    }
}
