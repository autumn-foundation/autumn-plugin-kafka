//! Kafka producer and consumers for [Autumn](https://autumn-web.app) apps.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use autumn_plugin_kafka::{Consumer, HandlerError, KafkaPlugin, KafkaProducer, Message, Record};
//! use autumn_web::prelude::*;
//!
//! #[derive(serde::Serialize, serde::Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[post("/orders/{id}")]
//! async fn create(producer: KafkaProducer, Path(id): Path<u64>) -> AutumnResult<&'static str> {
//!     producer
//!         .send(Record::json("orders", &Order { id })?.with_key(id.to_string()))
//!         .await?;
//!     Ok("queued")
//! }
//!
//! async fn on_order(msg: Message, _state: AppState) -> Result<(), HandlerError> {
//!     let order: Order = msg.json()?;
//!     tracing::info!(id = order.id, "order received");
//!     Ok(())
//! }
//!
//! #[autumn_web::main]
//! async fn main() {
//!     autumn_web::app()
//!         .plugin(
//!             KafkaPlugin::new().consumer(
//!                 Consumer::new("orders", ["orders"])
//!                     .group_id("billing")
//!                     .handler(on_order)
//!                     .dead_letter_topic("orders.dlq"),
//!             ),
//!         )
//!         .routes(routes![create])
//!         .run()
//!         .await;
//! }
//! ```
//!
//! # Parts
//!
//! | Part | Function |
//! |---|---|
//! | [`KafkaPlugin`] | Installs all parts. Reads `[kafka]` at startup. |
//! | [`KafkaConfig`] | The `[kafka]` config section. |
//! | [`KafkaProducer`] | Sends records. It is a handler argument. |
//! | [`Consumer`] | A consumer binding with an async handler. |
//! | [`MemoryBroker`] | An in-memory broker for tests. |
//! | [`RdKafkaBackend`] | The default client, `librdkafka`. |
//!
//! The plugin also adds the `kafka` health indicator and `kafka_*` counters
//! to `/actuator/prometheus`.
//!
//! # Delivery
//!
//! Consumers are at-least-once. The plugin commits an offset only after the
//! handler completes, or after the dead-letter send completes. Thus a handler
//! must be safe to run again for the same message.

mod backend;
mod config;
mod consumer;
mod error;
mod health;
mod memory;
mod message;
mod metrics;
mod plugin;
mod producer;
mod rdkafka_backend;

pub use backend::{Backend, ConsumerBackend, ConsumerSpec, ProducerBackend};
pub use config::{ClientRole, ConsumerSettings, HealthSettings, KafkaConfig, ProducerSettings};
pub use consumer::{
    Consumer, DLQ_HEADER_CONSUMER, DLQ_HEADER_ERROR, DLQ_HEADER_OFFSET, DLQ_HEADER_PARTITION,
    DLQ_HEADER_TOPIC, HandlerError,
};
pub use error::KafkaError;
/// The future type of the backend traits.
pub use futures::future::BoxFuture;
pub use memory::MemoryBroker;
pub use message::{Delivery, Header, Message, Record};
pub use plugin::{KafkaPlugin, KafkaRuntime, PLUGIN_NAME};
pub use producer::KafkaProducer;
pub use rdkafka_backend::RdKafkaBackend;
