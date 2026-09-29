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
    pub fn new(_topic: impl Into<String>, _payload: impl Into<Vec<u8>>) -> Self {
        todo!()
    }

    /// Makes a record with a JSON payload.
    ///
    /// The record gets the header `content-type: application/json`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Json`] if `value` cannot be encoded.
    pub fn json<T: Serialize + ?Sized>(
        _topic: impl Into<String>,
        _value: &T,
    ) -> Result<Self, KafkaError> {
        todo!()
    }

    /// Makes a record with no payload (a tombstone) for a key.
    #[must_use]
    pub fn tombstone(_topic: impl Into<String>, _key: impl Into<Vec<u8>>) -> Self {
        todo!()
    }

    /// Sets the key.
    #[must_use]
    pub fn key(self, _key: impl Into<Vec<u8>>) -> Self {
        todo!()
    }

    /// Adds a header.
    #[must_use]
    pub fn header(self, _name: impl Into<String>, _value: impl Into<Vec<u8>>) -> Self {
        todo!()
    }

    /// Returns the topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        todo!()
    }

    /// Returns the key.
    #[must_use]
    pub fn key_bytes(&self) -> Option<&[u8]> {
        todo!()
    }

    /// Returns the payload. `None` is a tombstone.
    #[must_use]
    pub fn payload(&self) -> Option<&[u8]> {
        todo!()
    }

    /// Returns all headers in order.
    #[must_use]
    pub fn headers(&self) -> &[Header] {
        todo!()
    }
}

/// The position of a record after the broker accepted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    /// The partition.
    pub partition: i32,
    /// The offset in the partition.
    pub offset: i64,
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
    pub fn new(_topic: impl Into<String>, _payload: impl Into<Vec<u8>>) -> Self {
        todo!()
    }

    /// Makes a message with no payload (a tombstone).
    #[must_use]
    pub fn tombstone(_topic: impl Into<String>) -> Self {
        todo!()
    }

    /// Sets the partition and the offset.
    #[must_use]
    pub const fn with_position(self, _partition: i32, _offset: i64) -> Self {
        todo!()
    }

    /// Sets the key.
    #[must_use]
    pub fn with_key(self, _key: impl Into<Vec<u8>>) -> Self {
        todo!()
    }

    /// Adds a header.
    #[must_use]
    pub fn with_header(self, _name: impl Into<String>, _value: impl Into<Vec<u8>>) -> Self {
        todo!()
    }

    /// Sets the timestamp in milliseconds since the Unix epoch.
    #[must_use]
    pub const fn with_timestamp_ms(self, _timestamp_ms: i64) -> Self {
        todo!()
    }

    /// Returns the topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        todo!()
    }

    /// Returns the partition.
    #[must_use]
    pub const fn partition(&self) -> i32 {
        todo!()
    }

    /// Returns the offset.
    #[must_use]
    pub const fn offset(&self) -> i64 {
        todo!()
    }

    /// Returns the key.
    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        todo!()
    }

    /// Returns the payload. A tombstone gives an empty slice.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        todo!()
    }

    /// Returns `true` if the message has no payload.
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        todo!()
    }

    /// Returns all headers in order.
    #[must_use]
    pub fn headers(&self) -> &[Header] {
        todo!()
    }

    /// Returns the last value of a header.
    #[must_use]
    pub fn header(&self, _name: &str) -> Option<&[u8]> {
        todo!()
    }

    /// Returns the timestamp in milliseconds since the Unix epoch.
    #[must_use]
    pub const fn timestamp_ms(&self) -> Option<i64> {
        todo!()
    }

    /// Decodes the payload as JSON.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Json`] if the payload is not valid for `T`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, KafkaError> {
        todo!()
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
        assert_eq!(record.payload(), Some(&b"hi"[..]));
        assert_eq!(record.key_bytes(), None);
        assert!(record.headers().is_empty());
    }

    #[test]
    fn record_builder_sets_key_and_headers() {
        let record = Record::new("t", "p")
            .key("k")
            .header("a", "1")
            .header("b", "2");
        assert_eq!(record.key_bytes(), Some(&b"k"[..]));
        assert_eq!(
            record.headers(),
            &[("a".to_owned(), b"1".to_vec()), ("b".to_owned(), b"2".to_vec())]
        );
    }

    #[test]
    fn record_json_encodes_and_sets_content_type() {
        let record = Record::json("orders", &Order { id: 7 }).unwrap();
        assert_eq!(record.payload(), Some(&br#"{"id":7}"#[..]));
        assert_eq!(
            record.headers(),
            &[("content-type".to_owned(), b"application/json".to_vec())]
        );
    }

    #[test]
    fn record_tombstone_has_key_and_no_payload() {
        let record = Record::tombstone("t", "k");
        assert_eq!(record.payload(), None);
        assert_eq!(record.key_bytes(), Some(&b"k"[..]));
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
