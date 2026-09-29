//! Kafka plugin for Autumn.

pub mod config;
pub mod error;
pub mod message;

pub use config::{ClientRole, KafkaConfig};
pub use error::KafkaError;
pub use message::{Delivery, Header, Message, Record};
