//! Kafka plugin for Autumn.

pub mod config;
pub mod error;

pub use config::{ClientRole, KafkaConfig};
pub use error::KafkaError;
