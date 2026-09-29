//! Kafka plugin for Autumn.

pub mod backend;
pub mod config;
mod consumer;
pub mod error;
mod health;
mod memory;
pub mod message;
mod metrics;
mod producer;
mod rdkafka_backend;

pub use backend::{Backend, ConsumerBackend, ConsumerSpec, ProducerBackend};
pub use config::{ClientRole, KafkaConfig};
pub use consumer::{
    Consumer, DLQ_HEADER_CONSUMER, DLQ_HEADER_ERROR, DLQ_HEADER_OFFSET, DLQ_HEADER_PARTITION,
    DLQ_HEADER_TOPIC, HandlerError,
};
pub use error::KafkaError;
pub use memory::MemoryBroker;
pub use message::{Delivery, Header, Message, Record};
pub use producer::KafkaProducer;
pub use rdkafka_backend::RdKafkaBackend;
