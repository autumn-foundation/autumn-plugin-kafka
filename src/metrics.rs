//! Counters for `/actuator/prometheus` and `/actuator/metrics`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use autumn_web::actuator::{MetricFamily, MetricKind, MetricSample, MetricsSource};

/// Counters for one consumer.
#[derive(Debug, Default)]
pub struct ConsumerCounters {
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
    /// `true` while the receive loop runs.
    pub running: AtomicBool,
}

/// All plugin counters. Registered as the metrics source `kafka`.
#[derive(Debug, Default)]
pub struct KafkaMetrics {
    /// Records that the broker accepted.
    pub produced: AtomicU64,
    /// Records that the broker did not accept.
    pub produce_errors: AtomicU64,
    consumers: Vec<(String, Arc<ConsumerCounters>)>,
}

impl KafkaMetrics {
    /// Makes counters for the named consumers.
    pub fn new<I, S>(consumer_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            consumers: consumer_names
                .into_iter()
                .map(|name| (name.into(), Arc::default()))
                .collect(),
            ..Self::default()
        }
    }

    /// Returns the counters of all consumers.
    pub fn consumers(&self) -> &[(String, Arc<ConsumerCounters>)] {
        &self.consumers
    }

    /// Returns the counters of a consumer.
    pub fn consumer(&self, name: &str) -> Option<Arc<ConsumerCounters>> {
        self.consumers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, c)| Arc::clone(c))
    }
}

impl MetricsSource for KafkaMetrics {
    fn collect(&self) -> Vec<MetricFamily> {
        let mut families = vec![
            counter(
                "kafka_messages_produced_total",
                "Records that the broker accepted",
                vec![sample(vec![], &self.produced)],
            ),
            counter(
                "kafka_produce_errors_total",
                "Records that the broker did not accept",
                vec![sample(vec![], &self.produce_errors)],
            ),
        ];
        let per_consumer: [(&str, &str, CounterField); 5] = [
            (
                "kafka_messages_consumed_total",
                "Messages that the handler processed",
                |c| &c.consumed,
            ),
            (
                "kafka_handler_errors_total",
                "Handler attempts that failed",
                |c| &c.handler_errors,
            ),
            (
                "kafka_messages_dead_lettered_total",
                "Messages sent to the dead-letter topic",
                |c| &c.dead_lettered,
            ),
            (
                "kafka_messages_skipped_total",
                "Messages skipped after all retries",
                |c| &c.skipped,
            ),
            (
                "kafka_receive_errors_total",
                "Client errors while the consumer waits for messages",
                |c| &c.receive_errors,
            ),
        ];
        for (name, help, field) in per_consumer {
            let samples = self
                .consumers
                .iter()
                .map(|(consumer, c)| {
                    sample(vec![("consumer".to_owned(), consumer.clone())], field(c))
                })
                .collect();
            families.push(counter(name, help, samples));
        }
        families.push(MetricFamily {
            name: "kafka_consumer_running".to_owned(),
            help: "1 while the consumer loop runs, else 0".to_owned(),
            kind: MetricKind::Gauge,
            samples: self
                .consumers
                .iter()
                .map(|(consumer, c)| MetricSample {
                    labels: vec![("consumer".to_owned(), consumer.clone())],
                    value: if c.running.load(Ordering::Relaxed) {
                        1.0
                    } else {
                        0.0
                    },
                })
                .collect(),
        });
        families
    }
}

/// Selects one counter of a consumer.
type CounterField = fn(&ConsumerCounters) -> &AtomicU64;

fn counter(name: &str, help: &str, samples: Vec<MetricSample>) -> MetricFamily {
    MetricFamily {
        name: name.to_owned(),
        help: help.to_owned(),
        kind: MetricKind::Counter,
        samples,
    }
}

#[allow(clippy::cast_precision_loss)] // Counters stay far below 2^53.
fn sample(labels: Vec<(String, String)>, value: &AtomicU64) -> MetricSample {
    MetricSample {
        labels,
        value: value.load(Ordering::Relaxed) as f64,
    }
}

#[cfg(test)]
mod tests {
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
            .find(|s| {
                consumer.map_or(s.labels.is_empty(), |c| {
                    s.labels == [("consumer".to_owned(), c.to_owned())]
                })
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
            assert!(
                (value(f, Some("orders")) - expected).abs() < f64::EPSILON,
                "{name}"
            );
            assert!(value(f, Some("audit")).abs() < f64::EPSILON, "{name}");
        }
    }

    #[test]
    fn collect_reports_running_consumers_as_a_gauge() {
        let metrics = KafkaMetrics::new(["up", "down"]);
        metrics
            .consumer("up")
            .unwrap()
            .running
            .store(true, Ordering::Relaxed);

        let families = metrics.collect();

        let running = family(&families, "kafka_consumer_running");
        assert!(matches!(running.kind, MetricKind::Gauge));
        assert!((value(running, Some("up")) - 1.0).abs() < f64::EPSILON);
        assert!(value(running, Some("down")).abs() < f64::EPSILON);
    }

    #[test]
    fn unknown_consumer_has_no_counters() {
        let metrics = KafkaMetrics::new(["a"]);
        assert!(metrics.consumer("b").is_none());
    }
}
