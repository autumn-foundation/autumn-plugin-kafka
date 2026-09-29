//! Consumer bindings and the receive loop.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::reexports::tokio_util::sync::CancellationToken;
use futures::future::BoxFuture;

use crate::backend::{ConsumerBackend, ConsumerSpec};
use crate::config::KafkaConfig;
use crate::error::KafkaError;
use crate::message::Message;
use crate::metrics::ConsumerCounters;
use crate::producer::KafkaProducer;

/// The error type that a handler can return.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;

type Handler =
    Arc<dyn Fn(Message, AppState) -> BoxFuture<'static, Result<(), HandlerError>> + Send + Sync>;

/// Dead-letter header: the consumer name.
pub const DLQ_HEADER_CONSUMER: &str = "autumn.dlq.consumer";
/// Dead-letter header: the source topic.
pub const DLQ_HEADER_TOPIC: &str = "autumn.dlq.original-topic";
/// Dead-letter header: the source partition, as decimal text.
pub const DLQ_HEADER_PARTITION: &str = "autumn.dlq.original-partition";
/// Dead-letter header: the source offset, as decimal text.
pub const DLQ_HEADER_OFFSET: &str = "autumn.dlq.original-offset";
/// Dead-letter header: the last handler error. Maximum 1024 bytes.
pub const DLQ_HEADER_ERROR: &str = "autumn.dlq.error";

/// A consumer binding: topics, a group, and a handler.
///
/// The consumer processes one message at a time, in order.
/// Delivery is at-least-once: a handler can see a message again after a restart.
///
/// ```rust,ignore
/// Consumer::new("orders", ["orders.placed"])
///     .group_id("billing")
///     .handler(|msg: Message, _state: AppState| async move {
///         let order: Order = msg.json()?;
///         bill(order).await
///     })
///     .max_retries(5)
///     .dead_letter_topic("orders.placed.dlq")
/// ```
#[derive(Clone)]
pub struct Consumer {
    name: String,
    topics: Vec<String>,
    group_id: Option<String>,
    properties: BTreeMap<String, String>,
    handler: Option<Handler>,
    max_retries: u32,
    retry_backoff: Duration,
    dead_letter_topic: Option<String>,
}

impl Consumer {
    /// Makes a binding. The name is the metrics label and must be unique.
    #[must_use]
    pub fn new<I, S>(_name: impl Into<String>, _topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        todo!()
    }

    /// Sets the consumer group. The default is `kafka.group_id`.
    #[must_use]
    pub fn group_id(self, _group_id: impl Into<String>) -> Self {
        todo!()
    }

    /// Sets a `librdkafka` property for this consumer only.
    #[must_use]
    pub fn property(self, _key: impl Into<String>, _value: impl Into<String>) -> Self {
        todo!()
    }

    /// Sets the handler. The handler gets the message and the app state.
    #[must_use]
    pub fn handler<F, Fut, E>(self, _handler: F) -> Self
    where
        F: Fn(Message, AppState) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<HandlerError>,
    {
        todo!()
    }

    /// Sets the number of retries after the first failure. Default: 3.
    #[must_use]
    pub const fn max_retries(self, _max_retries: u32) -> Self {
        todo!()
    }

    /// Sets the first retry delay. The delay doubles for each retry,
    /// to a maximum of 30 seconds. Default: 100 ms.
    #[must_use]
    pub const fn retry_backoff(self, _retry_backoff: Duration) -> Self {
        todo!()
    }

    /// Sends failed messages to this topic after all retries.
    ///
    /// Without a dead-letter topic, the consumer logs an error and skips the message.
    #[must_use]
    pub fn dead_letter_topic(self, _topic: impl Into<String>) -> Self {
        todo!()
    }

    /// Returns the name.
    #[must_use]
    pub fn name(&self) -> &str {
        todo!()
    }

    /// Returns the backend data for this binding.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Config`] if the binding has no group.
    pub(crate) fn spec(&self, _config: &KafkaConfig) -> Result<ConsumerSpec, KafkaError> {
        todo!()
    }
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("name", &self.name)
            .field("topics", &self.topics)
            .field("group_id", &self.group_id)
            .field("has_handler", &self.handler.is_some())
            .field("max_retries", &self.max_retries)
            .field("retry_backoff", &self.retry_backoff)
            .field("dead_letter_topic", &self.dead_letter_topic)
            .finish_non_exhaustive()
    }
}

/// Makes sure that all bindings can work together.
///
/// # Errors
///
/// Returns [`KafkaError::Config`] with the first problem found.
pub(crate) fn validate_consumers(
    _consumers: &[Consumer],
    _config: &KafkaConfig,
) -> Result<(), KafkaError> {
    todo!()
}

/// Receives and processes messages until `shutdown` is cancelled.
pub(crate) async fn run_consumer(
    _backend: Box<dyn ConsumerBackend>,
    _consumer: Consumer,
    _producer: KafkaProducer,
    _state: AppState,
    _counters: Arc<ConsumerCounters>,
    _shutdown: CancellationToken,
) {
    todo!()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::backend::Backend;
    use crate::memory::MemoryBroker;
    use crate::message::Record;
    use crate::metrics::KafkaMetrics;

    const GROUP: &str = "g";

    /// A running consumer loop for one test.
    struct Harness {
        broker: MemoryBroker,
        metrics: Arc<KafkaMetrics>,
        shutdown: CancellationToken,
        task: tokio::task::JoinHandle<()>,
    }

    impl Harness {
        fn start(broker: &MemoryBroker, consumer: Consumer) -> Self {
            let config = KafkaConfig::default();
            let metrics = Arc::new(KafkaMetrics::new([consumer.name().to_owned()]));
            let producer = KafkaProducer::new(
                broker.producer(&config).unwrap(),
                Duration::from_secs(1),
                Arc::clone(&metrics),
            );
            let backend = broker
                .consumer(&config, &consumer.spec(&config).unwrap())
                .unwrap();
            let counters = metrics.consumer(consumer.name()).unwrap();
            let shutdown = CancellationToken::new();
            let task = tokio::spawn(run_consumer(
                backend,
                consumer,
                producer,
                AppState::detached(),
                counters,
                shutdown.clone(),
            ));
            Self {
                broker: broker.clone(),
                metrics,
                shutdown,
                task,
            }
        }

        fn counters(&self) -> Arc<ConsumerCounters> {
            self.metrics.consumer("c").unwrap()
        }

        fn committed(&self) -> Option<i64> {
            self.broker.committed_offset(GROUP, "in")
        }

        async fn stop(self) {
            self.shutdown.cancel();
            tokio::time::timeout(Duration::from_secs(5), self.task)
                .await
                .expect("the loop stops")
                .unwrap();
        }
    }

    async fn wait_until(what: &str, check: impl Fn() -> bool) {
        for _ in 0..500 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out: {what}");
    }

    fn load(counter: &std::sync::atomic::AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    fn base() -> Consumer {
        Consumer::new("c", ["in"])
            .group_id(GROUP)
            .retry_backoff(Duration::from_millis(1))
    }

    /// A handler that fails for the first `failures` calls.
    fn flaky(failures: u32, calls: &Arc<AtomicU32>) -> Consumer {
        let calls = Arc::clone(calls);
        base().handler(move |_msg, _state| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < failures {
                    Err(format!("failure {n}"))
                } else {
                    Ok(())
                }
            }
        })
    }

    #[test]
    fn builder_sets_defaults_and_values() {
        let c = Consumer::new("c", ["a", "b"]);
        let text = format!("{c:?}");
        assert_eq!(c.name(), "c");
        assert!(text.contains("max_retries: 3"), "{text}");
        assert!(text.contains("retry_backoff: 100ms"), "{text}");
        assert!(text.contains("has_handler: false"), "{text}");
    }

    #[test]
    fn spec_uses_binding_group_first() {
        let config = KafkaConfig {
            group_id: Some("default".into()),
            ..KafkaConfig::default()
        };
        let own = Consumer::new("c", ["t"]).group_id("own").property("k", "v");
        let spec = own.spec(&config).unwrap();
        assert_eq!(spec.group_id, "own");
        assert_eq!(spec.name, "c");
        assert_eq!(spec.topics, ["t"]);
        assert_eq!(spec.properties["k"], "v");

        let fallback = Consumer::new("c", ["t"]).spec(&config).unwrap();
        assert_eq!(fallback.group_id, "default");
    }

    #[test]
    fn spec_fails_with_no_group() {
        let err = Consumer::new("c", ["t"])
            .spec(&KafkaConfig::default())
            .unwrap_err();
        assert!(err.to_string().contains("group"), "{err}");
    }

    #[test]
    fn validate_rejects_bad_bindings() {
        let ok = || {
            Consumer::new("c", ["t"])
                .group_id("g")
                .handler(|_, _| async { Ok::<_, HandlerError>(()) })
        };
        let config = KafkaConfig::default();
        let cases: Vec<(&str, Vec<Consumer>)> = vec![
            (
                "name",
                vec![Consumer {
                    name: " ".into(),
                    ..ok()
                }],
            ),
            (
                "topic",
                vec![
                    Consumer::new("c", Vec::<String>::new())
                        .group_id("g")
                        .handler(|_, _| async { Ok::<_, HandlerError>(()) }),
                ],
            ),
            (
                "topic",
                vec![Consumer {
                    topics: vec![String::new()],
                    ..ok()
                }],
            ),
            ("handler", vec![Consumer::new("c", ["t"]).group_id("g")]),
            (
                "group",
                vec![Consumer {
                    group_id: None,
                    ..ok()
                }],
            ),
            ("dead-letter", vec![ok().dead_letter_topic("t")]),
            ("dead-letter", vec![ok().dead_letter_topic(" ")]),
            ("unique", vec![ok(), ok()]),
            (
                "share",
                vec![
                    ok(),
                    Consumer {
                        name: "d".into(),
                        ..ok()
                    },
                ],
            ),
        ];
        for (needle, consumers) in cases {
            let err = validate_consumers(&consumers, &config).unwrap_err();
            assert!(err.to_string().contains(needle), "{needle}: {err}");
        }
    }

    #[test]
    fn validate_accepts_distinct_groups_on_one_topic() {
        let make = |name: &str, group: &str| {
            Consumer::new(name, ["t"])
                .group_id(group)
                .handler(|_, _| async { Ok::<_, HandlerError>(()) })
        };
        let consumers = [make("a", "g1"), make("b", "g2")];
        validate_consumers(&consumers, &KafkaConfig::default()).unwrap();
    }

    #[tokio::test]
    async fn success_commits_and_counts() {
        let broker = MemoryBroker::new();
        let calls = Arc::new(AtomicU32::new(0));
        let h = Harness::start(&broker, flaky(0, &calls));

        broker.publish(Record::new("in", "a"));
        broker.publish(Record::new("in", "b"));

        wait_until("two commits", || h.committed() == Some(2)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(load(&h.counters().consumed), 2);
        assert_eq!(load(&h.counters().handler_errors), 0);
        h.stop().await;
    }

    #[tokio::test]
    async fn handler_gets_message_and_state() {
        let broker = MemoryBroker::new();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let consumer = base().handler(move |msg: Message, state: AppState| {
            sink.lock()
                .unwrap()
                .push((msg.payload().to_vec(), state.profile().to_owned()));
            async { Ok::<_, HandlerError>(()) }
        });
        let h = Harness::start(&broker, consumer);

        broker.publish(Record::new("in", "hello"));

        wait_until("one commit", || h.committed() == Some(1)).await;
        assert_eq!(seen.lock().unwrap()[0].0, b"hello");
        h.stop().await;
    }

    #[tokio::test]
    async fn failure_retries_then_succeeds() {
        let broker = MemoryBroker::new();
        let calls = Arc::new(AtomicU32::new(0));
        let h = Harness::start(&broker, flaky(2, &calls).max_retries(3));

        broker.publish(Record::new("in", "a"));

        wait_until("commit", || h.committed() == Some(1)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(load(&h.counters().handler_errors), 2);
        assert_eq!(load(&h.counters().consumed), 1);
        assert_eq!(load(&h.counters().skipped), 0);
        h.stop().await;
    }

    #[tokio::test]
    async fn exhausted_retries_skip_and_continue() {
        let broker = MemoryBroker::new();
        let calls = Arc::new(AtomicU32::new(0));
        // Fails 3 times: the first message uses all attempts (1 + 2 retries).
        let h = Harness::start(&broker, flaky(3, &calls).max_retries(2));

        broker.publish(Record::new("in", "bad"));
        broker.publish(Record::new("in", "good"));

        wait_until("both commits", || h.committed() == Some(2)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(load(&h.counters().skipped), 1);
        assert_eq!(load(&h.counters().consumed), 1);
        h.stop().await;
    }

    #[tokio::test]
    async fn exhausted_retries_go_to_dead_letter_topic() {
        let broker = MemoryBroker::new();
        let calls = Arc::new(AtomicU32::new(0));
        let consumer = flaky(u32::MAX, &calls)
            .max_retries(1)
            .dead_letter_topic("in.dlq");
        let h = Harness::start(&broker, consumer);

        broker.publish(Record::new("in", "payload").key("k").header("trace", "t1"));

        wait_until("commit", || h.committed() == Some(1)).await;
        let dead = broker.messages("in.dlq");
        assert_eq!(dead.len(), 1);
        let d = &dead[0];
        assert_eq!(d.payload(), b"payload");
        assert_eq!(d.key(), Some(&b"k"[..]));
        assert_eq!(d.header("trace"), Some(&b"t1"[..]));
        assert_eq!(d.header(DLQ_HEADER_CONSUMER), Some(&b"c"[..]));
        assert_eq!(d.header(DLQ_HEADER_TOPIC), Some(&b"in"[..]));
        assert_eq!(d.header(DLQ_HEADER_PARTITION), Some(&b"0"[..]));
        assert_eq!(d.header(DLQ_HEADER_OFFSET), Some(&b"0"[..]));
        assert_eq!(d.header(DLQ_HEADER_ERROR), Some(&b"failure 1"[..]));
        assert_eq!(load(&h.counters().dead_lettered), 1);
        assert_eq!(load(&h.counters().skipped), 0);
        h.stop().await;
    }

    #[tokio::test]
    async fn long_errors_are_cut_in_the_dead_letter_header() {
        let broker = MemoryBroker::new();
        let consumer = base()
            .max_retries(0)
            .dead_letter_topic("in.dlq")
            .handler(|_, _| async { Err::<(), _>("é".repeat(2000)) });
        let h = Harness::start(&broker, consumer);

        broker.publish(Record::new("in", "x"));

        wait_until("commit", || h.committed() == Some(1)).await;
        let dead = broker.messages("in.dlq");
        let error = dead[0].header(DLQ_HEADER_ERROR).unwrap();
        assert!(error.len() <= 1024);
        assert!(std::str::from_utf8(error).is_ok());
        h.stop().await;
    }

    #[tokio::test]
    async fn failed_dead_letter_send_does_not_commit() {
        let broker = MemoryBroker::new();
        let calls = Arc::new(AtomicU32::new(0));
        let consumer = flaky(u32::MAX, &calls)
            .max_retries(0)
            .dead_letter_topic("in.dlq");
        let h = Harness::start(&broker, consumer);
        broker.set_available(false);

        broker.publish(Record::new("in", "x"));

        wait_until("send errors", || load(&h.metrics.produce_errors) >= 2).await;
        assert_eq!(h.committed(), None);
        broker.set_available(true);
        wait_until("commit", || h.committed() == Some(1)).await;
        assert_eq!(broker.messages("in.dlq").len(), 1);
        h.stop().await;
    }

    #[tokio::test]
    async fn panic_is_a_failure() {
        let broker = MemoryBroker::new();
        let consumer = base().max_retries(1).handler(|msg: Message, _| async move {
            assert_ne!(msg.payload(), b"boom", "handler panic");
            Ok::<_, HandlerError>(())
        });
        let h = Harness::start(&broker, consumer);

        broker.publish(Record::new("in", "boom"));
        broker.publish(Record::new("in", "fine"));

        wait_until("both commits", || h.committed() == Some(2)).await;
        assert_eq!(load(&h.counters().handler_errors), 2);
        assert_eq!(load(&h.counters().skipped), 1);
        assert_eq!(load(&h.counters().consumed), 1);
        h.stop().await;
    }

    #[tokio::test]
    async fn sync_panic_in_handler_call_is_a_failure() {
        let broker = MemoryBroker::new();
        let consumer =
            base()
                .max_retries(0)
                .handler(|_, _| -> BoxFuture<'static, Result<(), HandlerError>> {
                    panic!("panic before the future")
                });
        let h = Harness::start(&broker, consumer);

        broker.publish(Record::new("in", "x"));

        wait_until("commit", || h.committed() == Some(1)).await;
        assert_eq!(load(&h.counters().skipped), 1);
        h.stop().await;
    }

    #[tokio::test]
    async fn shutdown_stops_an_idle_loop() {
        let broker = MemoryBroker::new();
        let h = Harness::start(&broker, flaky(0, &Arc::new(AtomicU32::new(0))));
        h.stop().await;
    }

    #[tokio::test]
    async fn shutdown_during_backoff_does_not_commit() {
        let broker = MemoryBroker::new();
        let calls = Arc::new(AtomicU32::new(0));
        let consumer = flaky(u32::MAX, &calls)
            .max_retries(5)
            .retry_backoff(Duration::from_secs(3600));
        let h = Harness::start(&broker, consumer);

        broker.publish(Record::new("in", "x"));
        wait_until("first call", || calls.load(Ordering::SeqCst) == 1).await;

        let broker = h.broker.clone();
        h.stop().await;
        assert_eq!(broker.committed_offset(GROUP, "in"), None);
    }

    /// A consumer backend that fails once, then gives one message.
    struct FailOnce {
        failed: bool,
        sent: bool,
        committed: Arc<AtomicU32>,
    }

    impl ConsumerBackend for FailOnce {
        fn recv(&mut self) -> BoxFuture<'_, Result<Message, KafkaError>> {
            Box::pin(async move {
                if !self.failed {
                    self.failed = true;
                    return Err(KafkaError::Client("broker gone".into()));
                }
                if !self.sent {
                    self.sent = true;
                    return Ok(Message::new("in", "x"));
                }
                futures::future::pending().await
            })
        }

        fn commit(&mut self, _message: &Message) -> Result<(), KafkaError> {
            self.committed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    #[tokio::test(start_paused = true)]
    async fn receive_error_is_counted_and_the_loop_continues() {
        let broker = MemoryBroker::new();
        let metrics = Arc::new(KafkaMetrics::new(["c"]));
        let producer = KafkaProducer::new(
            broker.producer(&KafkaConfig::default()).unwrap(),
            Duration::from_secs(1),
            Arc::clone(&metrics),
        );
        let committed = Arc::new(AtomicU32::new(0));
        let backend = Box::new(FailOnce {
            failed: false,
            sent: false,
            committed: Arc::clone(&committed),
        });
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run_consumer(
            backend,
            flaky(0, &Arc::new(AtomicU32::new(0))),
            producer,
            AppState::detached(),
            metrics.consumer("c").unwrap(),
            shutdown.clone(),
        ));

        wait_until("commit", || committed.load(Ordering::SeqCst) == 1).await;
        assert_eq!(load(&metrics.consumer("c").unwrap().receive_errors), 1);
        shutdown.cancel();
        task.await.unwrap();
    }
}
