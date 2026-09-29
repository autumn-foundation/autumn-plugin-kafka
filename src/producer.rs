//! The producer handle for handlers, jobs, and consumers.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::error::AutumnError;
use autumn_web::reexports::axum::extract::FromRequestParts;
use autumn_web::reexports::http::request::Parts;

use crate::backend::ProducerBackend;
use crate::error::KafkaError;
use crate::message::{Delivery, Record};
use crate::metrics::KafkaMetrics;

/// Extra time after the client send timeout.
const SEND_MARGIN: Duration = Duration::from_millis(500);

/// Sends records to Kafka. Clones share one client.
///
/// Use it as a handler argument:
///
/// ```rust,ignore
/// #[post("/orders")]
/// async fn create(producer: KafkaProducer, Json(order): Json<Order>) -> AutumnResult<()> {
///     producer.send(Record::json("orders", &order)?.with_key(order.id.to_string())).await?;
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct KafkaProducer {
    backend: Arc<dyn ProducerBackend>,
    send_timeout: Duration,
    metrics: Arc<KafkaMetrics>,
}

impl KafkaProducer {
    pub(crate) fn new(
        backend: Arc<dyn ProducerBackend>,
        send_timeout: Duration,
        metrics: Arc<KafkaMetrics>,
    ) -> Self {
        Self {
            backend,
            send_timeout,
            metrics,
        }
    }

    /// Sends a record. Completes when the broker accepts it.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Timeout`] after `producer.send_timeout_ms` plus 500 ms.
    /// The broker can still get the record after a timeout.
    /// Returns other errors from the client.
    pub async fn send(&self, record: Record) -> Result<Delivery, KafkaError> {
        // The client timeout ends first. The margin lets the client report the
        // result, so that a caller does not send again a record that the broker
        // can still accept.
        let result = tokio::time::timeout(
            self.send_timeout + SEND_MARGIN,
            self.backend.send(record, self.send_timeout),
        )
        .await
        .unwrap_or(Err(KafkaError::Timeout));
        let counter = if result.is_ok() {
            &self.metrics.produced
        } else {
            &self.metrics.produce_errors
        };
        counter.fetch_add(1, Ordering::Relaxed);
        result
    }

    /// Waits until all queued records are sent.
    pub(crate) async fn flush(&self, timeout: Duration) -> Result<(), KafkaError> {
        tokio::time::timeout(timeout, self.backend.flush(timeout))
            .await
            .unwrap_or(Err(KafkaError::Timeout))
    }

    /// Returns the producer backend. The health indicator uses it.
    pub(crate) fn backend(&self) -> Arc<dyn ProducerBackend> {
        Arc::clone(&self.backend)
    }

    /// Returns the producer that the plugin installed, if the plugin started.
    #[must_use]
    pub fn from_state(state: &AppState) -> Option<Self> {
        state.extension::<Self>().map(|p| (*p).clone())
    }
}

impl std::fmt::Debug for KafkaProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaProducer")
            .field("send_timeout", &self.send_timeout)
            .finish_non_exhaustive()
    }
}

impl FromRequestParts<AppState> for KafkaProducer {
    type Rejection = AutumnError;

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Self::from_state(state)
            .ok_or_else(|| AutumnError::service_unavailable_msg("Kafka producer is not started"))
    }
}

#[cfg(test)]
mod tests {
    use futures::future::BoxFuture;

    use super::*;
    use crate::backend::Backend;
    use crate::config::KafkaConfig;
    use crate::memory::MemoryBroker;

    fn producer(broker: &MemoryBroker) -> (KafkaProducer, Arc<KafkaMetrics>) {
        let metrics = Arc::new(KafkaMetrics::new(Vec::<String>::new()));
        let backend = broker.producer(&KafkaConfig::default()).unwrap();
        let producer = KafkaProducer::new(backend, Duration::from_secs(1), Arc::clone(&metrics));
        (producer, metrics)
    }

    /// A backend that never completes a send.
    struct Stuck;

    impl ProducerBackend for Stuck {
        fn send(&self, _: Record, _: Duration) -> BoxFuture<'_, Result<Delivery, KafkaError>> {
            Box::pin(futures::future::pending())
        }
        fn ping(&self, _: Duration) -> BoxFuture<'_, Result<(), KafkaError>> {
            Box::pin(async { Ok(()) })
        }
        fn flush(&self, _: Duration) -> BoxFuture<'_, Result<(), KafkaError>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn send_delivers_and_counts() {
        let broker = MemoryBroker::new();
        let (producer, metrics) = producer(&broker);

        let delivery = producer.send(Record::new("t", "x")).await.unwrap();

        assert_eq!(delivery.offset, 0);
        assert_eq!(broker.messages("t").len(), 1);
        assert_eq!(metrics.produced.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.produce_errors.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn send_failure_counts_an_error() {
        let broker = MemoryBroker::new();
        let (producer, metrics) = producer(&broker);
        broker.set_available(false);

        let err = producer.send(Record::new("t", "x")).await.unwrap_err();

        assert!(matches!(err, KafkaError::Unavailable(_)), "{err}");
        assert_eq!(metrics.produced.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.produce_errors.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn send_waits_a_margin_past_the_client_timeout() {
        let metrics = Arc::new(KafkaMetrics::new(Vec::<String>::new()));
        let producer = KafkaProducer::new(
            Arc::new(Stuck),
            Duration::from_millis(100),
            Arc::clone(&metrics),
        );
        let start = tokio::time::Instant::now();

        let err = producer.send(Record::new("t", "x")).await.unwrap_err();

        assert!(matches!(err, KafkaError::Timeout), "{err}");
        assert!(
            start.elapsed() >= Duration::from_millis(600),
            "{:?}",
            start.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn send_times_out() {
        let metrics = Arc::new(KafkaMetrics::new(Vec::<String>::new()));
        let producer = KafkaProducer::new(
            Arc::new(Stuck),
            Duration::from_millis(100),
            Arc::clone(&metrics),
        );

        let err = producer.send(Record::new("t", "x")).await.unwrap_err();

        assert!(matches!(err, KafkaError::Timeout), "{err}");
        assert_eq!(metrics.produce_errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn from_state_finds_the_installed_producer() {
        let state = AppState::detached();
        assert!(KafkaProducer::from_state(&state).is_none());

        let (producer, _) = producer(&MemoryBroker::new());
        state.insert_extension(producer);

        assert!(KafkaProducer::from_state(&state).is_some());
    }

    #[tokio::test]
    async fn extractor_rejects_with_503_when_not_started() {
        let state = AppState::detached();
        let (mut parts, ()) = autumn_web::reexports::http::Request::new(()).into_parts();

        let err = KafkaProducer::from_request_parts(&mut parts, &state)
            .await
            .unwrap_err();

        assert_eq!(err.status().as_u16(), 503);
    }

    #[tokio::test]
    async fn extractor_returns_the_installed_producer() {
        let state = AppState::detached();
        let (producer, _) = producer(&MemoryBroker::new());
        state.insert_extension(producer);
        let (mut parts, ()) = autumn_web::reexports::http::Request::new(()).into_parts();

        let result = KafkaProducer::from_request_parts(&mut parts, &state).await;

        assert!(result.is_ok());
    }
}
