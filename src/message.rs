//! Outgoing records and incoming messages.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::KafkaError;

/// A header name and value.
pub type Header = (String, Vec<u8>);

/// A record to send to Kafka.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    topic: String,
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    headers: Vec<Header>,
}

impl Record {
    /// Makes a record with a payload and no key.
    #[must_use]
    pub fn new(topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            key: None,
            payload: Some(payload.into()),
            headers: Vec::new(),
        }
    }

    /// Makes a record with a JSON payload.
    ///
    /// The record gets the header `content-type: application/json`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Json`] if `value` cannot be encoded.
    pub fn json<T: Serialize + ?Sized>(
        topic: impl Into<String>,
        value: &T,
    ) -> Result<Self, KafkaError> {
        let payload = serde_json::to_vec(value)?;
        Ok(Self::new(topic, payload).with_header("content-type", "application/json"))
    }

    /// Makes a record with no payload (a tombstone) for a key.
    #[must_use]
    pub fn tombstone(topic: impl Into<String>, key: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            key: Some(key.into()),
            payload: None,
            headers: Vec::new(),
        }
    }

    /// Makes a tombstone with no key. Kafka rejects it on a compacted topic.
    pub(crate) fn keyless_tombstone(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            key: None,
            payload: None,
            headers: Vec::new(),
        }
    }

    /// Sets the key.
    #[must_use]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Adds a header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Returns the topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the key.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Returns the payload. A tombstone gives an empty slice.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload.as_deref().unwrap_or_default()
    }

    /// Returns `true` if the record has no payload.
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        self.payload.is_none()
    }

    /// Returns all headers in order.
    #[must_use]
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }
}

/// The position of a record after the broker accepted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Delivery {
    /// The partition.
    pub partition: i32,
    /// The offset in the partition.
    pub offset: i64,
}

impl Delivery {
    /// Makes a delivery. Custom backends use it.
    #[must_use]
    pub const fn new(partition: i32, offset: i64) -> Self {
        Self { partition, offset }
    }
}

/// A message received from Kafka.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    topic: String,
    partition: i32,
    offset: i64,
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
    headers: Vec<Header>,
    timestamp_ms: Option<i64>,
}

impl Message {
    /// Makes a message at partition 0, offset 0. Use it in tests.
    #[must_use]
    pub fn new(topic: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            payload: Some(payload.into()),
            ..Self::tombstone(topic)
        }
    }

    /// Makes a message with no payload (a tombstone).
    #[must_use]
    pub fn tombstone(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partition: 0,
            offset: 0,
            key: None,
            payload: None,
            headers: Vec::new(),
            timestamp_ms: None,
        }
    }

    /// Sets the partition and the offset.
    #[must_use]
    pub const fn with_position(mut self, partition: i32, offset: i64) -> Self {
        self.partition = partition;
        self.offset = offset;
        self
    }

    /// Sets the key.
    #[must_use]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Adds a header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Sets the timestamp in milliseconds since the Unix epoch.
    #[must_use]
    pub const fn with_timestamp_ms(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }

    /// Returns the topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition.
    #[must_use]
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the offset.
    #[must_use]
    pub const fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the key.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Returns the payload. A tombstone gives an empty slice.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload.as_deref().unwrap_or_default()
    }

    /// Returns `true` if the message has no payload.
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        self.payload.is_none()
    }

    /// Returns all headers in order.
    #[must_use]
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    /// Returns the last value of a header.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_slice())
    }

    /// Returns the timestamp in milliseconds since the Unix epoch.
    #[must_use]
    pub const fn timestamp_ms(&self) -> Option<i64> {
        self.timestamp_ms
    }

    /// Makes a message from a record at a position. Moves all record fields.
    pub(crate) fn from_record(
        record: Record,
        partition: i32,
        offset: i64,
        timestamp_ms: Option<i64>,
    ) -> Self {
        Self {
            topic: record.topic,
            partition,
            offset,
            key: record.key,
            payload: record.payload,
            headers: record.headers,
            timestamp_ms,
        }
    }

    /// Decodes the payload as JSON.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Json`] if the payload is not valid for `T`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, KafkaError> {
        Ok(serde_json::from_slice(self.payload())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Order {
        id: u32,
    }

    #[test]
    fn record_new_has_payload_and_no_key() {
        let record = Record::new("orders", b"hi".to_vec());
        assert_eq!(record.topic(), "orders");
        assert_eq!(record.payload(), b"hi");
        assert!(!record.is_tombstone());
        assert_eq!(record.key(), None);
        assert!(record.headers().is_empty());
    }

    #[test]
    fn record_builder_sets_key_and_headers() {
        let record = Record::new("t", "p")
            .with_key("k")
            .with_header("a", "1")
            .with_header("b", "2");
        assert_eq!(record.key(), Some(&b"k"[..]));
        assert_eq!(
            record.headers(),
            &[
                ("a".to_owned(), b"1".to_vec()),
                ("b".to_owned(), b"2".to_vec())
            ]
        );
    }

    #[test]
    fn record_json_encodes_and_sets_content_type() {
        let record = Record::json("orders", &Order { id: 7 }).unwrap();
        assert_eq!(record.payload(), br#"{"id":7}"#);
        assert_eq!(
            record.headers(),
            &[("content-type".to_owned(), b"application/json".to_vec())]
        );
    }

    #[test]
    fn record_tombstone_has_key_and_no_payload() {
        let record = Record::tombstone("t", "k");
        assert!(record.is_tombstone());
        assert_eq!(record.payload(), b"");
        assert_eq!(record.key(), Some(&b"k"[..]));
    }

    #[test]
    fn message_builder_sets_all_fields() {
        let msg = Message::new("t", "p")
            .with_position(3, 42)
            .with_key("k")
            .with_header("h", "v")
            .with_timestamp_ms(1000);
        assert_eq!(msg.topic(), "t");
        assert_eq!(msg.partition(), 3);
        assert_eq!(msg.offset(), 42);
        assert_eq!(msg.key(), Some(&b"k"[..]));
        assert_eq!(msg.payload(), b"p");
        assert!(!msg.is_tombstone());
        assert_eq!(msg.headers().len(), 1);
        assert_eq!(msg.timestamp_ms(), Some(1000));
    }

    #[test]
    fn message_defaults_are_empty() {
        let msg = Message::new("t", "p");
        assert_eq!((msg.partition(), msg.offset()), (0, 0));
        assert_eq!(msg.key(), None);
        assert_eq!(msg.timestamp_ms(), None);
    }

    #[test]
    fn message_tombstone_has_empty_payload() {
        let msg = Message::tombstone("t");
        assert!(msg.is_tombstone());
        assert_eq!(msg.payload(), b"");
    }

    #[test]
    fn message_header_returns_last_value() {
        let msg = Message::new("t", "p")
            .with_header("h", "1")
            .with_header("x", "0")
            .with_header("h", "2");
        assert_eq!(msg.header("h"), Some(&b"2"[..]));
        assert_eq!(msg.header("missing"), None);
    }

    #[test]
    fn message_json_decodes_payload() {
        let msg = Message::new("t", r#"{"id":9}"#);
        assert_eq!(msg.json::<Order>().unwrap(), Order { id: 9 });
    }

    #[test]
    fn message_json_reports_bad_payload() {
        let err = Message::new("t", "nope").json::<Order>().unwrap_err();
        assert!(matches!(err, KafkaError::Json(_)), "{err}");
    }
}
