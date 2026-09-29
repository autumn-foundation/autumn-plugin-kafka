//! The `kafka` health indicator.

use std::sync::Arc;
use std::time::Duration;

use autumn_web::actuator::{HealthCheckOutput, HealthIndicator, IndicatorGroup};
use futures::future::BoxFuture;

use crate::backend::ProducerBackend;

/// Extra time for the framework timeout. The probe timeout ends first.
const MARGIN_MS: u64 = 500;

/// Reports `UP` if a broker replies to a metadata request in time.
pub struct KafkaHealth {
    producer: Arc<dyn ProducerBackend>,
    probe_timeout: Duration,
    readiness: bool,
}

impl KafkaHealth {
    /// Makes an indicator. If `readiness` is `true`, it also gates `/ready`.
    pub fn new(
        producer: Arc<dyn ProducerBackend>,
        probe_timeout: Duration,
        readiness: bool,
    ) -> Self {
        Self {
            producer,
            probe_timeout,
            readiness,
        }
    }
}

impl HealthIndicator for KafkaHealth {
    fn check(&self) -> BoxFuture<'_, HealthCheckOutput> {
        todo!()
    }

    fn timeout_ms(&self) -> u64 {
        todo!()
    }

    fn group(&self) -> IndicatorGroup {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use autumn_web::actuator::HealthStatus;

    use super::*;
    use crate::backend::Backend;
    use crate::config::KafkaConfig;
    use crate::error::KafkaError;
    use crate::memory::MemoryBroker;
    use crate::message::{Delivery, Record};

    fn health(broker: &MemoryBroker, readiness: bool) -> KafkaHealth {
        let producer = broker.producer(&KafkaConfig::default()).unwrap();
        KafkaHealth::new(producer, Duration::from_millis(200), readiness)
    }

    struct Silent;

    impl ProducerBackend for Silent {
        fn send(&self, _: Record, _: Duration) -> BoxFuture<'_, Result<Delivery, KafkaError>> {
            Box::pin(futures::future::pending())
        }
        fn ping(&self, _: Duration) -> BoxFuture<'_, Result<(), KafkaError>> {
            Box::pin(futures::future::pending())
        }
        fn flush(&self, _: Duration) -> BoxFuture<'_, Result<(), KafkaError>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn up_when_broker_replies() {
        let output = health(&MemoryBroker::new(), false).check().await;
        assert!(matches!(output.status, HealthStatus::Up));
    }

    #[tokio::test]
    async fn down_with_reason_when_broker_fails() {
        let broker = MemoryBroker::new();
        broker.set_available(false);

        let output = health(&broker, false).check().await;

        assert!(matches!(output.status, HealthStatus::Down));
        let error = output.details["error"].as_str().unwrap();
        assert!(error.contains("not available"), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn down_when_probe_times_out() {
        let indicator = KafkaHealth::new(Arc::new(Silent), Duration::from_millis(200), false);

        let output = indicator.check().await;

        assert!(matches!(output.status, HealthStatus::Down));
        let error = output.details["error"].as_str().unwrap();
        assert!(error.contains("timed out"), "{error}");
    }

    #[test]
    fn group_follows_readiness_flag() {
        let broker = MemoryBroker::new();
        assert!(matches!(
            health(&broker, false).group(),
            IndicatorGroup::HealthOnly
        ));
        assert!(matches!(
            health(&broker, true).group(),
            IndicatorGroup::Readiness
        ));
    }

    #[test]
    fn framework_timeout_is_longer_than_probe() {
        assert_eq!(health(&MemoryBroker::new(), false).timeout_ms(), 700);
    }
}
