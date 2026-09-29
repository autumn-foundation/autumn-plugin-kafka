//! Consumer bindings and the receive loop.

use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::reexports::tokio_util::sync::CancellationToken;
use futures::FutureExt as _;
use futures::future::BoxFuture;

use crate::backend::{ConsumerBackend, ConsumerSpec};
use crate::config::KafkaConfig;
use crate::error::KafkaError;
use crate::message::{Message, Record};
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
    pub fn new<I, S>(name: impl Into<String>, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            name: name.into(),
            topics: topics.into_iter().map(Into::into).collect(),
            group_id: None,
            properties: BTreeMap::new(),
            handler: None,
            max_retries: 3,
            retry_backoff: Duration::from_millis(100),
            dead_letter_topic: None,
        }
    }

    /// Sets the consumer group. The default is `kafka.group_id`.
    #[must_use]
    pub fn group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    /// Sets a `librdkafka` property for this consumer only.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Sets the handler. The handler gets the message and the app state.
    #[must_use]
    pub fn handler<F, Fut, E>(mut self, handler: F) -> Self
    where
        F: Fn(Message, AppState) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Into<HandlerError>,
    {
        self.handler = Some(Arc::new(move |msg, state| {
            let fut = handler(msg, state);
            Box::pin(async move { fut.await.map_err(Into::into) })
        }));
        self
    }

    /// Sets the number of retries after the first failure. Default: 3.
    #[must_use]
    pub const fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the first retry delay. The delay doubles for each retry,
    /// to a maximum of 30 seconds. Default: 100 ms.
    #[must_use]
    pub const fn retry_backoff(mut self, retry_backoff: Duration) -> Self {
        self.retry_backoff = retry_backoff;
        self
    }

    /// Sends failed messages to this topic after all retries.
    ///
    /// Without a dead-letter topic, the consumer logs an error and skips the message.
    #[must_use]
    pub fn dead_letter_topic(mut self, topic: impl Into<String>) -> Self {
        self.dead_letter_topic = Some(topic.into());
        self
    }

    /// Returns the name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the backend data for this binding.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::Config`] if the binding has no group.
    pub(crate) fn spec(&self, config: &KafkaConfig) -> Result<ConsumerSpec, KafkaError> {
        let group_id = self
            .group_id
            .clone()
            .or_else(|| config.group_id.clone())
            .ok_or_else(|| {
                KafkaError::Config(format!(
                    "consumer {:?} has no group: set Consumer::group_id or kafka.group_id",
                    self.name
                ))
            })?;
        Ok(ConsumerSpec {
            name: self.name.clone(),
            group_id,
            topics: self.topics.clone(),
            properties: self.properties.clone(),
        })
    }

    /// Returns the delay before retry number `attempt` (1-based).
    fn backoff(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1));
        self.retry_backoff.saturating_mul(factor).min(MAX_BACKOFF)
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
    consumers: &[Consumer],
    config: &KafkaConfig,
) -> Result<(), KafkaError> {
    let fail = |msg: String| Err(KafkaError::Config(msg));
    let mut names = HashSet::new();
    let mut subscriptions: HashMap<(String, &str), &str> = HashMap::new();
    for c in consumers {
        let name = &c.name;
        if name.trim().is_empty() {
            return fail("a consumer name must not be empty".to_owned());
        }
        if !names.insert(name.as_str()) {
            return fail(format!("consumer name {name:?} is not unique"));
        }
        if c.topics.is_empty() || c.topics.iter().any(|t| t.trim().is_empty()) {
            return fail(format!(
                "consumer {name:?} needs a topic, and no topic can be empty"
            ));
        }
        if c.handler.is_none() {
            return fail(format!("consumer {name:?} has no handler"));
        }
        if let Some(dlq) = &c.dead_letter_topic {
            if dlq.trim().is_empty() || c.topics.contains(dlq) {
                return fail(format!(
                    "consumer {name:?} has a bad dead-letter topic: it must not be empty \
                     or one of its own topics"
                ));
            }
        }
        let group = c.spec(config)?.group_id;
        if group.trim().is_empty() {
            return fail(format!("consumer {name:?} has an empty group"));
        }
        for topic in &c.topics {
            if let Some(other) = subscriptions.insert((group.clone(), topic.as_str()), name) {
                return fail(format!(
                    "consumers {other:?} and {name:?} share group {group:?} and topic {topic:?}; \
                     each would get only some partitions"
                ));
            }
        }
    }
    Ok(())
}

/// Receives and processes messages until `shutdown` is cancelled.
pub(crate) async fn run_consumer(
    mut backend: Box<dyn ConsumerBackend>,
    consumer: Consumer,
    producer: KafkaProducer,
    state: AppState,
    counters: Arc<ConsumerCounters>,
    shutdown: CancellationToken,
) {
    let Some(handler) = consumer.handler.clone() else {
        tracing::error!(consumer = %consumer.name, "Kafka consumer has no handler");
        backend.close().await;
        return;
    };
    let worker = Worker {
        consumer: &consumer,
        handler,
        producer: &producer,
        state: &state,
        counters: &counters,
        shutdown: &shutdown,
    };
    tracing::info!(consumer = %consumer.name, topics = ?consumer.topics, "Kafka consumer started");
    loop {
        let received = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            received = backend.recv() => received,
        };
        match received {
            Ok(msg) => {
                if worker.process(&mut *backend, &msg).await == Outcome::Cancelled {
                    break;
                }
            }
            Err(error) => {
                counters.receive_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(consumer = %consumer.name, %error, "Kafka receive failed");
                if sleep_or_cancel(RECEIVE_ERROR_BACKOFF, &shutdown).await {
                    break;
                }
            }
        }
    }
    backend.close().await;
    tracing::info!(consumer = %consumer.name, "Kafka consumer stopped");
}

/// The longest retry delay.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// The delay after a receive error.
const RECEIVE_ERROR_BACKOFF: Duration = Duration::from_secs(1);
/// The maximum length of the error header, in bytes.
const MAX_ERROR_HEADER_LEN: usize = 1024;

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// The message is committed, or the commit failed and was logged.
    Done,
    /// Shutdown started before the commit.
    Cancelled,
}

/// The parts that the loop uses to process one message.
struct Worker<'a> {
    consumer: &'a Consumer,
    handler: Handler,
    producer: &'a KafkaProducer,
    state: &'a AppState,
    counters: &'a ConsumerCounters,
    shutdown: &'a CancellationToken,
}

impl Worker<'_> {
    async fn process(&self, backend: &mut dyn ConsumerBackend, msg: &Message) -> Outcome {
        let mut attempt = 0;
        let error = loop {
            match self.invoke(msg).await {
                Ok(()) => {
                    self.counters.consumed.fetch_add(1, Ordering::Relaxed);
                    self.commit(backend, msg);
                    return Outcome::Done;
                }
                Err(error) => {
                    self.counters.handler_errors.fetch_add(1, Ordering::Relaxed);
                    if attempt >= self.consumer.max_retries {
                        break error;
                    }
                    attempt += 1;
                    tracing::warn!(
                        consumer = %self.consumer.name,
                        topic = msg.topic(),
                        partition = msg.partition(),
                        offset = msg.offset(),
                        attempt,
                        %error,
                        "Kafka handler failed; retry"
                    );
                    if sleep_or_cancel(self.consumer.backoff(attempt), self.shutdown).await {
                        return Outcome::Cancelled;
                    }
                }
            }
        };

        if let Some(topic) = &self.consumer.dead_letter_topic {
            if self.dead_letter(topic, msg, &error).await == Outcome::Cancelled {
                return Outcome::Cancelled;
            }
        } else {
            self.counters.skipped.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                consumer = %self.consumer.name,
                topic = msg.topic(),
                partition = msg.partition(),
                offset = msg.offset(),
                %error,
                "Kafka handler failed after all retries; message skipped"
            );
        }
        self.commit(backend, msg);
        Outcome::Done
    }

    /// Calls the handler. A panic becomes an error.
    async fn invoke(&self, msg: &Message) -> Result<(), HandlerError> {
        let call = std::panic::catch_unwind(AssertUnwindSafe(|| {
            (self.handler)(msg.clone(), self.state.clone())
        }));
        let fut = match call {
            Ok(fut) => fut,
            Err(panic) => return Err(panic_error(&panic)),
        };
        match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(result) => result,
            Err(panic) => Err(panic_error(&panic)),
        }
    }

    /// Sends the message to the dead-letter topic. Retries until success or shutdown.
    async fn dead_letter(&self, topic: &str, msg: &Message, error: &HandlerError) -> Outcome {
        let record = dead_letter_record(topic, &self.consumer.name, msg, error);
        let mut attempt = 0;
        loop {
            match self.producer.send(record.clone()).await {
                Ok(_) => {
                    self.counters.dead_lettered.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        consumer = %self.consumer.name,
                        topic = msg.topic(),
                        partition = msg.partition(),
                        offset = msg.offset(),
                        dead_letter_topic = topic,
                        %error,
                        "Kafka handler failed after all retries; message dead-lettered"
                    );
                    return Outcome::Done;
                }
                Err(send_error) => {
                    attempt += 1;
                    tracing::error!(
                        consumer = %self.consumer.name,
                        dead_letter_topic = topic,
                        error = %send_error,
                        "Kafka dead-letter send failed; retry"
                    );
                    if sleep_or_cancel(self.consumer.backoff(attempt), self.shutdown).await {
                        return Outcome::Cancelled;
                    }
                }
            }
        }
    }

    fn commit(&self, backend: &mut dyn ConsumerBackend, msg: &Message) {
        if let Err(error) = backend.commit(msg) {
            tracing::error!(
                consumer = %self.consumer.name,
                topic = msg.topic(),
                partition = msg.partition(),
                offset = msg.offset(),
                %error,
                "Kafka commit failed; the message can come again"
            );
        }
    }
}

fn dead_letter_record(topic: &str, consumer: &str, msg: &Message, error: &HandlerError) -> Record {
    let mut record = if msg.is_tombstone() {
        Record::tombstone(topic, msg.key().unwrap_or_default())
    } else {
        let record = Record::new(topic, msg.payload());
        match msg.key() {
            Some(key) => record.key(key),
            None => record,
        }
    };
    for (name, value) in msg.headers() {
        record = record.header(name.clone(), value.clone());
    }
    record
        .header(DLQ_HEADER_CONSUMER, consumer)
        .header(DLQ_HEADER_TOPIC, msg.topic())
        .header(DLQ_HEADER_PARTITION, msg.partition().to_string())
        .header(DLQ_HEADER_OFFSET, msg.offset().to_string())
        .header(
            DLQ_HEADER_ERROR,
            truncate(&error.to_string(), MAX_ERROR_HEADER_LEN),
        )
}

/// Cuts `text` to at most `max` bytes, on a character boundary.
fn truncate(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn panic_error(panic: &(dyn Any + Send)) -> HandlerError {
    let detail = panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_owned());
    format!("handler panicked: {detail}").into()
}

/// Sleeps for `delay`. Returns `true` if shutdown started first.
async fn sleep_or_cancel(delay: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => true,
        () = tokio::time::sleep(delay) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use super::*;
    use crate::backend::Backend;
    use crate::memory::MemoryBroker;
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
    async fn panic_text_goes_to_the_dead_letter_header() {
        let broker = MemoryBroker::new();
        let consumer = base()
            .max_retries(0)
            .dead_letter_topic("in.dlq")
            .handler(|_, _| async {
                let detail = String::from("detail 42");
                panic!("{detail}");
                #[allow(unreachable_code)]
                Ok::<_, HandlerError>(())
            });
        let h = Harness::start(&broker, consumer);

        broker.publish(Record::new("in", "x"));

        wait_until("commit", || h.committed() == Some(1)).await;
        let dead = broker.messages("in.dlq");
        let error = std::str::from_utf8(dead[0].header(DLQ_HEADER_ERROR).unwrap()).unwrap();
        assert_eq!(error, "handler panicked: detail 42");
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
