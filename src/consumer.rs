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

/// The prefix of all dead-letter headers.
const DLQ_HEADER_PREFIX: &str = "autumn.dlq.";
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

    /// Returns the sum of all retry delays for one message.
    fn retry_budget(&self) -> Duration {
        (1..=self.max_retries)
            .map(|attempt| self.backoff(attempt))
            .try_fold(Duration::ZERO, Duration::checked_add)
            .unwrap_or(Duration::MAX)
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
pub fn validate_consumers(consumers: &[Consumer], config: &KafkaConfig) -> Result<(), KafkaError> {
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
        if let Some(dlq) = &c.dead_letter_topic
            && (dlq.trim().is_empty() || c.topics.contains(dlq))
        {
            return fail(format!(
                "consumer {name:?} has a bad dead-letter topic: it must not be empty \
                 or one of its own topics"
            ));
        }
        if c.retry_backoff.is_zero() {
            return fail(format!(
                "consumer {name:?}: retry_backoff must be greater than 0"
            ));
        }
        let spec = c.spec(config)?;
        let group = spec.group_id;
        if group.trim().is_empty() {
            return fail(format!("consumer {name:?} has an empty group"));
        }
        let max_poll = spec
            .properties
            .get(MAX_POLL_INTERVAL)
            .or_else(|| config.consumer.properties.get(MAX_POLL_INTERVAL))
            .or_else(|| config.properties.get(MAX_POLL_INTERVAL))
            .map_or(Ok(DEFAULT_MAX_POLL_INTERVAL_MS), |v| {
                v.trim().parse::<u64>()
            })
            .map_err(|_| {
                KafkaError::Config(format!(
                    "consumer {name:?}: {MAX_POLL_INTERVAL} is not a number"
                ))
            })?;
        let budget = c.retry_budget();
        if budget >= Duration::from_millis(max_poll) {
            return fail(format!(
                "consumer {name:?}: the total retry delay ({} ms) must be less than \
                 {MAX_POLL_INTERVAL} ({max_poll} ms); the broker removes a consumer that \
                 does not poll in time",
                budget.as_millis()
            ));
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
///
/// The loop also stops if the client panics, or if the broker rejects a
/// dead-letter record. Then the consumer is not running, and health is `DOWN`.
pub async fn run_consumer(
    backend: Box<dyn ConsumerBackend>,
    consumer: Consumer,
    producer: KafkaProducer,
    state: AppState,
    counters: Arc<ConsumerCounters>,
    shutdown: CancellationToken,
) {
    let mut guard = LoopGuard {
        backend: Some(backend),
        counters: Arc::clone(&counters),
        shutdown: shutdown.clone(),
        name: consumer.name.clone(),
    };
    let Some(handler) = consumer.handler.clone() else {
        tracing::error!(consumer = %consumer.name, "Kafka consumer has no handler");
        guard.close().await;
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
    counters.running.store(true, Ordering::Relaxed);
    tracing::info!(consumer = %consumer.name, topics = ?consumer.topics, "Kafka consumer started");
    if let Some(backend) = guard.backend.as_deref_mut() {
        worker.receive_loop(backend).await;
    }
    guard.close().await;
    tracing::info!(consumer = %consumer.name, "Kafka consumer stopped");
}

/// Closes the backend and clears the running flag, also when the task is aborted.
struct LoopGuard {
    backend: Option<Box<dyn ConsumerBackend>>,
    counters: Arc<ConsumerCounters>,
    shutdown: CancellationToken,
    name: String,
}

impl LoopGuard {
    async fn close(&mut self) {
        if let Some(backend) = self.backend.take() {
            backend.close().await;
        }
    }
}

impl Drop for LoopGuard {
    fn drop(&mut self) {
        self.counters.running.store(false, Ordering::Relaxed);
        if !self.shutdown.is_cancelled() {
            tracing::error!(consumer = %self.name, "Kafka consumer stopped before shutdown");
        }
        // An aborted task did not close the backend. Close it off this thread,
        // because a client close can block.
        if let Some(backend) = self.backend.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(backend.close());
        }
    }
}

/// The `librdkafka` property that limits the time between two polls.
const MAX_POLL_INTERVAL: &str = "max.poll.interval.ms";
/// The `librdkafka` default of `max.poll.interval.ms`.
const DEFAULT_MAX_POLL_INTERVAL_MS: u64 = 300_000;
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
    /// A permanent error. The consumer must stop without a commit.
    Stopped,
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
    async fn receive_loop(&self, backend: &mut dyn ConsumerBackend) {
        loop {
            let received = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return,
                received = AssertUnwindSafe(backend.recv()).catch_unwind() => received,
            };
            match received {
                Ok(Ok(msg)) => {
                    if self.process(backend, &msg).await != Outcome::Done {
                        return;
                    }
                }
                Ok(Err(error)) => {
                    self.counters.receive_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(consumer = %self.consumer.name, %error, "Kafka receive failed");
                    if sleep_or_cancel(RECEIVE_ERROR_BACKOFF, self.shutdown).await {
                        return;
                    }
                }
                Err(panic) => {
                    // The client can lose the message in a panic. Stop, so that no later
                    // offset is committed over it.
                    self.counters.receive_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        consumer = %self.consumer.name,
                        error = %panic_error(panic.as_ref()),
                        "Kafka client panicked in receive; consumer stopped"
                    );
                    return;
                }
            }
        }
    }

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
            let outcome = self.dead_letter(topic, msg, &error).await;
            if outcome != Outcome::Done {
                return outcome;
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
            Err(panic) => return Err(panic_error(panic.as_ref())),
        };
        match AssertUnwindSafe(fut).catch_unwind().await {
            Ok(result) => result,
            Err(panic) => Err(panic_error(panic.as_ref())),
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
                Err(send_error @ KafkaError::Rejected(_)) => {
                    tracing::error!(
                        consumer = %self.consumer.name,
                        topic = msg.topic(),
                        partition = msg.partition(),
                        offset = msg.offset(),
                        dead_letter_topic = topic,
                        error = %send_error,
                        "Kafka rejected the dead-letter record; consumer stopped with no commit"
                    );
                    return Outcome::Stopped;
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
        Record::keyless_tombstone(topic)
    } else {
        Record::new(topic, msg.payload())
    };
    if let Some(key) = msg.key() {
        record = record.with_key(key);
    }
    // Drop incoming plugin headers, so that a producer cannot forge them.
    for (name, value) in msg.headers() {
        if !name.starts_with(DLQ_HEADER_PREFIX) {
            record = record.with_header(name.clone(), value.clone());
        }
    }
    record
        .with_header(DLQ_HEADER_CONSUMER, consumer)
        .with_header(DLQ_HEADER_TOPIC, msg.topic())
        .with_header(DLQ_HEADER_PARTITION, msg.partition().to_string())
        .with_header(DLQ_HEADER_OFFSET, msg.offset().to_string())
        .with_header(
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
                "no group",
                vec![Consumer {
                    group_id: None,
                    ..ok()
                }],
            ),
            ("empty group", vec![ok().group_id(" ")]),
            ("retry_backoff", vec![ok().retry_backoff(Duration::ZERO)]),
            (
                "max.poll.interval.ms",
                vec![ok().property("max.poll.interval.ms", "1000").max_retries(5)],
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
    fn validate_uses_config_max_poll_interval() {
        let mut config = KafkaConfig::default();
        config
            .consumer
            .properties
            .insert("max.poll.interval.ms".into(), "500".into());
        let consumer = Consumer::new("c", ["t"])
            .group_id("g")
            .max_retries(3)
            .handler(|_, _| async { Ok::<_, HandlerError>(()) });

        let err = validate_consumers(&[consumer], &config).unwrap_err();

        assert!(err.to_string().contains("max.poll.interval.ms"), "{err}");
    }

    #[test]
    fn validate_accepts_one_group_on_many_topics() {
        let make = |name: &str, topic: &str| {
            Consumer::new(name, [topic])
                .group_id("g")
                .handler(|_, _| async { Ok::<_, HandlerError>(()) })
        };
        let consumers = [make("a", "t1"), make("b", "t2")];
        validate_consumers(&consumers, &KafkaConfig::default()).unwrap();
    }

    #[test]
    fn backoff_doubles_and_stops_at_30_seconds() {
        let c = Consumer::new("c", ["t"]).retry_backoff(Duration::from_millis(100));
        assert_eq!(c.backoff(1), Duration::from_millis(100));
        assert_eq!(c.backoff(2), Duration::from_millis(200));
        assert_eq!(c.backoff(3), Duration::from_millis(400));
        assert_eq!(c.backoff(20), MAX_BACKOFF);
        assert_eq!(c.backoff(u32::MAX), MAX_BACKOFF);
        let huge = Consumer::new("c", ["t"]).retry_backoff(Duration::MAX);
        assert_eq!(huge.backoff(1), MAX_BACKOFF);
    }

    #[test]
    fn dead_letter_record_keeps_tombstones_and_null_keys() {
        let error: HandlerError = "e".into();
        let no_key = dead_letter_record("dlq", "c", &Message::tombstone("in"), &error);
        assert!(no_key.is_tombstone());
        assert_eq!(no_key.key(), None);

        let msg = Message::tombstone("in").with_key("k");
        let with_key = dead_letter_record("dlq", "c", &msg, &error);
        assert!(with_key.is_tombstone());
        assert_eq!(with_key.key(), Some(&b"k"[..]));
    }

    #[test]
    fn dead_letter_record_replaces_incoming_dlq_headers() {
        let msg = Message::new("in", "p")
            .with_header(DLQ_HEADER_TOPIC, "forged")
            .with_header("autumn.dlq.other", "x")
            .with_header("keep", "v");

        let record = dead_letter_record("dlq", "c", &msg, &"e".into());

        let topics: Vec<_> = record
            .headers()
            .iter()
            .filter(|(n, _)| n == DLQ_HEADER_TOPIC)
            .collect();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].1, b"in");
        assert!(
            record
                .headers()
                .iter()
                .all(|(n, _)| n != "autumn.dlq.other")
        );
        assert!(record.headers().iter().any(|(n, _)| n == "keep"));
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

        broker.publish(
            Record::new("in", "payload")
                .with_key("k")
                .with_header("trace", "t1"),
        );

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
    async fn tombstone_goes_to_the_dead_letter_topic_as_a_tombstone() {
        let broker = MemoryBroker::new();
        let calls = Arc::new(AtomicU32::new(0));
        let consumer = flaky(u32::MAX, &calls)
            .max_retries(0)
            .dead_letter_topic("in.dlq");
        let h = Harness::start(&broker, consumer);

        broker.publish(Record::tombstone("in", "k"));

        wait_until("commit", || h.committed() == Some(1)).await;
        let dead = broker.messages("in.dlq");
        assert!(dead[0].is_tombstone());
        assert_eq!(dead[0].key(), Some(&b"k"[..]));
        h.stop().await;
    }

    #[tokio::test]
    async fn shutdown_during_dead_letter_outage_does_not_commit() {
        let broker = MemoryBroker::new();
        let calls = Arc::new(AtomicU32::new(0));
        let consumer = flaky(u32::MAX, &calls)
            .max_retries(0)
            .dead_letter_topic("in.dlq");
        let h = Harness::start(&broker, consumer);
        broker.set_available(false);

        broker.publish(Record::new("in", "x"));
        wait_until("a send error", || load(&h.metrics.produce_errors) >= 1).await;
        let broker = h.broker.clone();
        h.stop().await;

        assert_eq!(broker.committed_offset(GROUP, "in"), None);
        assert!(broker.messages("in.dlq").is_empty());
    }

    #[tokio::test]
    async fn rejected_dead_letter_send_stops_the_consumer() {
        let broker = MemoryBroker::new();
        broker.reject_topic("in.dlq");
        let calls = Arc::new(AtomicU32::new(0));
        let consumer = flaky(u32::MAX, &calls)
            .max_retries(0)
            .dead_letter_topic("in.dlq");
        let h = Harness::start(&broker, consumer);

        broker.publish(Record::new("in", "x"));
        broker.publish(Record::new("in", "y"));

        let counters = h.counters();
        tokio::time::timeout(Duration::from_secs(5), h.task)
            .await
            .expect("the loop stops by itself")
            .unwrap();
        assert_eq!(broker.committed_offset(GROUP, "in"), None);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!counters.running.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn running_flag_follows_the_loop() {
        let broker = MemoryBroker::new();
        let h = Harness::start(&broker, flaky(0, &Arc::new(AtomicU32::new(0))));
        let counters = h.counters();

        wait_until("running", || counters.running.load(Ordering::Relaxed)).await;
        h.stop().await;

        assert!(!counters.running.load(Ordering::Relaxed));
    }

    /// A consumer backend that panics in `recv`.
    struct PanicRecv;

    impl ConsumerBackend for PanicRecv {
        fn recv(&mut self) -> BoxFuture<'_, Result<Message, KafkaError>> {
            Box::pin(async { panic!("bad header") })
        }

        fn commit(&mut self, _message: &Message) -> Result<(), KafkaError> {
            Ok(())
        }

        fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    fn test_producer(broker: &MemoryBroker, metrics: &Arc<KafkaMetrics>) -> KafkaProducer {
        KafkaProducer::new(
            broker.producer(&KafkaConfig::default()).unwrap(),
            Duration::from_secs(1),
            Arc::clone(metrics),
        )
    }

    #[tokio::test]
    async fn receive_panic_stops_the_consumer() {
        let broker = MemoryBroker::new();
        let metrics = Arc::new(KafkaMetrics::new(["c"]));
        let counters = metrics.consumer("c").unwrap();
        let task = tokio::spawn(run_consumer(
            Box::new(PanicRecv),
            flaky(0, &Arc::new(AtomicU32::new(0))),
            test_producer(&broker, &metrics),
            AppState::detached(),
            Arc::clone(&counters),
            CancellationToken::new(),
        ));

        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the loop stops by itself");

        assert!(result.is_ok(), "the panic does not leave the task");
        assert!(!counters.running.load(Ordering::Relaxed));
        assert_eq!(load(&counters.receive_errors), 1);
    }

    /// A consumer backend that counts `close` calls.
    struct CloseProbe {
        inner: Box<dyn ConsumerBackend>,
        closed: Arc<AtomicU32>,
    }

    impl ConsumerBackend for CloseProbe {
        fn recv(&mut self) -> BoxFuture<'_, Result<Message, KafkaError>> {
            self.inner.recv()
        }

        fn commit(&mut self, message: &Message) -> Result<(), KafkaError> {
            self.inner.commit(message)
        }

        fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
            self.closed.fetch_add(1, Ordering::SeqCst);
            self.inner.close()
        }
    }

    #[tokio::test]
    async fn aborted_loop_still_closes_the_backend() {
        let broker = MemoryBroker::new();
        let metrics = Arc::new(KafkaMetrics::new(["c"]));
        let started = Arc::new(tokio::sync::Notify::new());
        let signal = Arc::clone(&started);
        let consumer = base().handler(move |_, _| {
            signal.notify_one();
            futures::future::pending::<Result<(), HandlerError>>()
        });
        let closed = Arc::new(AtomicU32::new(0));
        let config = KafkaConfig::default();
        let backend = Box::new(CloseProbe {
            inner: broker
                .consumer(&config, &consumer.spec(&config).unwrap())
                .unwrap(),
            closed: Arc::clone(&closed),
        });
        let task = tokio::spawn(run_consumer(
            backend,
            consumer,
            test_producer(&broker, &metrics),
            AppState::detached(),
            metrics.consumer("c").unwrap(),
            CancellationToken::new(),
        ));
        broker.publish(Record::new("in", "x"));
        started.notified().await;

        task.abort();

        wait_until("close", || closed.load(Ordering::SeqCst) == 1).await;
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

        let start = tokio::time::Instant::now();
        wait_until("commit", || committed.load(Ordering::SeqCst) == 1).await;
        assert!(start.elapsed() >= RECEIVE_ERROR_BACKOFF);
        assert_eq!(load(&metrics.consumer("c").unwrap().receive_errors), 1);
        shutdown.cancel();
        task.await.unwrap();
    }
}
