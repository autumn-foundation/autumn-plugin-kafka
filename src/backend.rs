//! The interface between the plugin and a Kafka client.
//!
//! [`RdKafkaBackend`](crate::RdKafkaBackend) is the default.
//! [`MemoryBroker`](crate::MemoryBroker) is for tests.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

use crate::config::KafkaConfig;
use crate::error::KafkaError;
use crate::message::{Delivery, Message, Record};

/// Makes producers and consumers.
pub trait Backend: Send + Sync + 'static {
    /// Makes a producer.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot be made.
    fn producer(&self, config: &KafkaConfig) -> Result<Arc<dyn ProducerBackend>, KafkaError>;

    /// Makes a consumer and subscribes it to the topics.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot be made.
    fn consumer(
        &self,
        config: &KafkaConfig,
        spec: &ConsumerSpec,
    ) -> Result<Box<dyn ConsumerBackend>, KafkaError>;
}

/// The data that a backend needs to make one consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConsumerSpec {
    /// The consumer name.
    pub name: String,
    /// The consumer group.
    pub group_id: String,
    /// The topics to subscribe to.
    pub topics: Vec<String>,
    /// Properties for this consumer only. They override the config.
    pub properties: BTreeMap<String, String>,
}

impl ConsumerSpec {
    /// Makes a spec with no extra properties.
    #[must_use]
    pub fn new<I, S>(name: impl Into<String>, group_id: impl Into<String>, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            name: name.into(),
            group_id: group_id.into(),
            topics: topics.into_iter().map(Into::into).collect(),
            properties: BTreeMap::new(),
        }
    }

    /// Adds a property for this consumer only.
    #[must_use]
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

/// Sends records.
pub trait ProducerBackend: Send + Sync + 'static {
    /// Sends one record. Completes when the broker accepts it.
    fn send(
        &self,
        record: Record,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<Delivery, KafkaError>>;

    /// Makes sure that a broker replies.
    fn ping(&self, timeout: Duration) -> BoxFuture<'_, Result<(), KafkaError>>;

    /// Waits until all queued records are sent.
    fn flush(&self, timeout: Duration) -> BoxFuture<'_, Result<(), KafkaError>>;
}

/// Receives messages for one consumer group.
pub trait ConsumerBackend: Send + 'static {
    /// Waits for the next message.
    ///
    /// The future must be safe to drop before it completes.
    fn recv(&mut self) -> BoxFuture<'_, Result<Message, KafkaError>>;

    /// Marks a message as processed.
    ///
    /// After a restart, the group continues after the last marked message.
    ///
    /// # Errors
    ///
    /// Returns an error if the client rejects the offset.
    fn commit(&mut self, message: &Message) -> Result<(), KafkaError>;

    /// Leaves the group and releases the client.
    fn close(self: Box<Self>) -> BoxFuture<'static, ()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_debug_redacts_secrets() {
        let spec = ConsumerSpec::new("c", "g", ["t"]).with_property("sasl.password", "hunter2");
        let text = format!("{spec:?}");
        assert!(!text.contains("hunter2"), "{text}");
        assert!(text.contains("sasl.password"), "{text}");
    }
}
