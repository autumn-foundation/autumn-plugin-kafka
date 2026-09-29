//! The `kafka` health indicator.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_web::actuator::{HealthCheckOutput, HealthIndicator, IndicatorGroup};
use futures::future::BoxFuture;

use crate::backend::ProducerBackend;
use crate::error::KafkaError;
use crate::metrics::ConsumerCounters;

/// Extra time for the framework timeout. The probe timeout ends first.
const MARGIN_MS: u64 = 500;

/// Reports `UP` if a broker replies to a metadata request in time,
/// and all consumers run.
pub struct KafkaHealth {
    producer: Arc<dyn ProducerBackend>,
    consumers: Vec<(String, Arc<ConsumerCounters>)>,
    probe_timeout: Duration,
    readiness: bool,
}

impl KafkaHealth {
    /// Makes an indicator. If `readiness` is `true`, it also gates `/ready`.
    pub fn new(
        producer: Arc<dyn ProducerBackend>,
        consumers: Vec<(String, Arc<ConsumerCounters>)>,
        probe_timeout: Duration,
        readiness: bool,
    ) -> Self {
        Self {
            producer,
            consumers,
            probe_timeout,
            readiness,
        }
    }
}

impl HealthIndicator for KafkaHealth {
    fn check(&self) -> BoxFuture<'_, HealthCheckOutput> {
        Box::pin(async move {
            let probe = self.producer.ping(self.probe_timeout);
            let result = tokio::time::timeout(self.probe_timeout, probe)
                .await
                .unwrap_or(Err(KafkaError::Timeout));
            match result {
                Ok(()) => HealthCheckOutput::up(),
                Err(error) => HealthCheckOutput::down().with_details(HashMap::from([(
                    "error".to_owned(),
                    serde_json::Value::String(error.to_string()),
                )])),
            }
        })
    }

    fn timeout_ms(&self) -> u64 {
        u64::try_from(self.probe_timeout.as_millis())
            .unwrap_or(u64::MAX)
            .saturating_add(MARGIN_MS)
    }

    fn group(&self) -> IndicatorGroup {
        if self.readiness {
            IndicatorGroup::Readiness
        } else {
            IndicatorGroup::HealthOnly
        }
    }
}

#[cfg(test)]
mod tests {
    use autumn_web::actuator::HealthStatus;

    use super::*;
    use crate::backend::Backend;
    use crate::config::KafkaConfig;
    use crate::memory::MemoryBroker;
    use crate::message::{Delivery, Record};

    fn health(broker: &MemoryBroker, readiness: bool) -> KafkaHealth {
        let producer = broker.producer(&KafkaConfig::default()).unwrap();
        KafkaHealth::new(producer, vec![], Duration::from_millis(200), readiness)
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
        let indicator =
            KafkaHealth::new(Arc::new(Silent), vec![], Duration::from_millis(200), false);

        let output = indicator.check().await;

        assert!(matches!(output.status, HealthStatus::Down));
        let error = output.details["error"].as_str().unwrap();
        assert!(error.contains("timed out"), "{error}");
    }

    #[tokio::test]
    async fn down_when_a_consumer_stopped() {
        let broker = MemoryBroker::new();
        let producer = broker.producer(&KafkaConfig::default()).unwrap();
        let up = Arc::new(ConsumerCounters::default());
        up.running.store(true, std::sync::atomic::Ordering::Relaxed);
        let down = Arc::new(ConsumerCounters::default());
        let indicator = KafkaHealth::new(
            producer,
            vec![("a".into(), up), ("b".into(), down)],
            Duration::from_millis(200),
            false,
        );

        let output = indicator.check().await;

        assert!(matches!(output.status, HealthStatus::Down));
        assert_eq!(
            output.details["stopped_consumers"],
            serde_json::json!(["b"])
        );
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
