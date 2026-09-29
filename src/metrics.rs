//! Counters for `/actuator/prometheus` and `/actuator/metrics`.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use autumn_web::actuator::{MetricFamily, MetricsSource};

/// Counters for one consumer.
#[derive(Debug, Default)]
pub(crate) struct ConsumerCounters {
    /// Messages that the handler processed.
    pub consumed: AtomicU64,
    /// Handler attempts that failed.
    pub handler_errors: AtomicU64,
    /// Messages sent to the dead-letter topic.
    pub dead_lettered: AtomicU64,
    /// Messages skipped after all retries.
    pub skipped: AtomicU64,
    /// Errors from the client when it receives.
    pub receive_errors: AtomicU64,
}

/// All plugin counters. Registered as the metrics source `kafka`.
#[derive(Debug, Default)]
pub(crate) struct KafkaMetrics {
    /// Records that the broker accepted.
    pub produced: AtomicU64,
    /// Records that the broker did not accept.
    pub produce_errors: AtomicU64,
    consumers: Vec<(String, Arc<ConsumerCounters>)>,
}

impl KafkaMetrics {
    /// Makes counters for the named consumers.
    pub fn new<I, S>(_consumer_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        todo!()
    }

    /// Returns the counters of a consumer.
    pub fn consumer(&self, _name: &str) -> Option<Arc<ConsumerCounters>> {
        todo!()
    }
}

impl MetricsSource for KafkaMetrics {
    fn collect(&self) -> Vec<MetricFamily> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use autumn_web::actuator::MetricKind;

    use super::*;

    fn family<'a>(families: &'a [MetricFamily], name: &str) -> &'a MetricFamily {
        families
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("no family {name}"))
    }

    fn value(family: &MetricFamily, consumer: Option<&str>) -> f64 {
        family
            .samples
            .iter()
            .find(|s| match consumer {
                None => s.labels.is_empty(),
                Some(c) => s.labels == [("consumer".to_owned(), c.to_owned())],
            })
            .map(|s| s.value)
            .expect("a sample")
    }

    #[test]
    fn collect_reports_producer_counters() {
        let metrics = KafkaMetrics::new(Vec::<String>::new());
        metrics.produced.fetch_add(3, Ordering::Relaxed);
        metrics.produce_errors.fetch_add(1, Ordering::Relaxed);

        let families = metrics.collect();

        let produced = family(&families, "kafka_messages_produced_total");
        assert!(matches!(produced.kind, MetricKind::Counter));
        assert!(!produced.help.is_empty());
        assert!((value(produced, None) - 3.0).abs() < f64::EPSILON);
        let errors = family(&families, "kafka_produce_errors_total");
        assert!((value(errors, None) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn collect_reports_each_consumer_with_a_label() {
        let metrics = KafkaMetrics::new(["orders", "audit"]);
        let orders = metrics.consumer("orders").unwrap();
        orders.consumed.fetch_add(5, Ordering::Relaxed);
        orders.handler_errors.fetch_add(4, Ordering::Relaxed);
        orders.dead_lettered.fetch_add(3, Ordering::Relaxed);
        orders.skipped.fetch_add(2, Ordering::Relaxed);
        orders.receive_errors.fetch_add(1, Ordering::Relaxed);

        let families = metrics.collect();

        for (name, expected) in [
            ("kafka_messages_consumed_total", 5.0),
            ("kafka_handler_errors_total", 4.0),
            ("kafka_messages_dead_lettered_total", 3.0),
            ("kafka_messages_skipped_total", 2.0),
            ("kafka_receive_errors_total", 1.0),
        ] {
            let f = family(&families, name);
            assert!(matches!(f.kind, MetricKind::Counter), "{name}");
            assert!((value(f, Some("orders")) - expected).abs() < f64::EPSILON, "{name}");
            assert!(value(f, Some("audit")).abs() < f64::EPSILON, "{name}");
        }
    }

    #[test]
    fn unknown_consumer_has_no_counters() {
        let metrics = KafkaMetrics::new(["a"]);
        assert!(metrics.consumer("b").is_none());
    }
}
