//! Kafka plugin for Autumn.

pub mod backend;
pub mod config;
pub mod error;
mod memory;
pub mod message;
mod metrics;
mod producer;

pub use backend::{Backend, ConsumerBackend, ConsumerSpec, ProducerBackend};
pub use config::{ClientRole, KafkaConfig};
pub use error::KafkaError;
pub use memory::MemoryBroker;
pub use message::{Delivery, Header, Message, Record};
pub use producer::KafkaProducer;
