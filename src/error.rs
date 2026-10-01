//! Error type for the crate.

/// An error from the Kafka plugin.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KafkaError {
    /// The configuration is not valid.
    #[error("invalid Kafka config: {0}")]
    Config(String),
    /// The Kafka client returned an error.
    #[error("Kafka client error: {0}")]
    Client(String),
    /// JSON encode or decode failed.
    #[error("Kafka JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// The operation did not complete in time.
    #[error("Kafka operation timed out")]
    Timeout,
    /// The broker rejected the request. A retry does not help.
    ///
    /// Examples: the record is too large, or the client has no permission.
    #[error("Kafka rejected the request: {0}")]
    Rejected(String),
    /// The broker or the plugin is not available.
    #[error("Kafka is not available: {0}")]
    Unavailable(String),
}
