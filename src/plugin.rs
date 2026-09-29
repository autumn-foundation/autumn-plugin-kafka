//! The plugin and its runtime.

use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::app::AppBuilder;
use autumn_web::plugin::Plugin;
use autumn_web::reexports::tokio_util::sync::CancellationToken;
use tokio::task::JoinHandle;

use crate::backend::Backend;
use crate::config::KafkaConfig;
use crate::consumer::Consumer;
use crate::error::KafkaError;
use crate::metrics::KafkaMetrics;
use crate::producer::KafkaProducer;
use crate::rdkafka_backend::RdKafkaBackend;

/// The plugin name.
pub const PLUGIN_NAME: &str = "autumn-plugin-kafka";

/// The Kafka plugin.
///
/// ```rust,ignore
/// autumn_web::app()
///     .plugin(
///         KafkaPlugin::new()
///             .consumer(Consumer::new("orders", ["orders.placed"]).handler(on_order)),
///     )
///     .run()
///     .await;
/// ```
///
/// At startup, the plugin:
///
/// 1. Loads `[kafka]` (see [`KafkaConfig::load`]), if you did not call [`config`](Self::config).
/// 2. Makes sure that the config and the consumers are valid. If not, the app does not start.
/// 3. Installs a [`KafkaProducer`] as an app extension.
/// 4. Registers the `kafka` health indicator.
/// 5. Starts one task for each consumer.
///
/// At shutdown, it stops the consumers and flushes the producer.
pub struct KafkaPlugin {
    config: Option<KafkaConfig>,
    backend: Arc<dyn Backend>,
    consumers: Vec<Consumer>,
    runtime: KafkaRuntime,
}

impl KafkaPlugin {
    /// Makes a plugin with the [`RdKafkaBackend`] and no consumers.
    #[must_use]
    pub fn new() -> Self {
        todo!()
    }

    /// Uses this config. Then the plugin does not read `[kafka]`.
    #[must_use]
    pub fn config(self, _config: KafkaConfig) -> Self {
        todo!()
    }

    /// Uses another backend. For example, use [`MemoryBroker`](crate::MemoryBroker) in tests.
    #[must_use]
    pub fn backend(self, _backend: impl Backend) -> Self {
        todo!()
    }

    /// Adds a consumer.
    #[must_use]
    pub fn consumer(self, _consumer: Consumer) -> Self {
        todo!()
    }

    /// Returns a handle to the runtime. Use it to stop the consumers in tests.
    #[must_use]
    pub fn runtime(&self) -> KafkaRuntime {
        todo!()
    }
}

impl Default for KafkaPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for KafkaPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaPlugin")
            .field("config", &self.config)
            .field("consumers", &self.consumers)
            .finish_non_exhaustive()
    }
}

impl Plugin for KafkaPlugin {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed(PLUGIN_NAME)
    }

    fn build(self, _app: AppBuilder) -> AppBuilder {
        todo!()
    }
}

/// A handle to the running plugin. Clones share one runtime.
#[derive(Clone, Default)]
pub struct KafkaRuntime {
    inner: Arc<RuntimeInner>,
}

#[derive(Default)]
struct RuntimeInner {
    shutdown: CancellationToken,
    state: Mutex<RuntimeState>,
}

#[derive(Default)]
struct RuntimeState {
    started: bool,
    producer: Option<KafkaProducer>,
    tasks: Vec<JoinHandle<()>>,
    shutdown_timeout: Duration,
}

impl KafkaRuntime {
    /// Returns the producer, after startup.
    #[must_use]
    pub fn producer(&self) -> Option<KafkaProducer> {
        todo!()
    }

    /// Returns `true` after startup and before shutdown.
    #[must_use]
    pub fn is_running(&self) -> bool {
        todo!()
    }

    /// Stops the consumers, then flushes the producer.
    ///
    /// A consumer completes its current message first.
    /// After `shutdown_timeout_ms`, the function stops the remaining tasks.
    /// It is safe to call more than one time.
    pub async fn shutdown(&self) {
        todo!()
    }

    /// Starts the plugin parts. The startup hook calls this function.
    pub(crate) fn start(
        &self,
        _state: &AppState,
        _config: KafkaConfig,
        _backend: &dyn Backend,
        _consumers: Vec<Consumer>,
        _metrics: &Arc<KafkaMetrics>,
    ) -> Result<(), KafkaError> {
        todo!()
    }
}

impl std::fmt::Debug for KafkaRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaRuntime")
            .field("running", &self.is_running())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use autumn_web::actuator::HealthStatus;

    use super::*;
    use crate::consumer::HandlerError;
    use crate::memory::MemoryBroker;
    use crate::message::Record;

    fn metrics(consumers: &[Consumer]) -> Arc<KafkaMetrics> {
        Arc::new(KafkaMetrics::new(consumers.iter().map(Consumer::name)))
    }

    fn counting(calls: &Arc<AtomicU32>) -> Consumer {
        let calls = Arc::clone(calls);
        Consumer::new("c", ["in"])
            .group_id("g")
            .handler(move |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, HandlerError>(()) }
            })
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

    #[test]
    fn build_declares_config_section_and_name() {
        let app = autumn_web::app().plugin(KafkaPlugin::new());
        assert!(app.has_config_section("kafka"));
        assert!(app.has_plugin(PLUGIN_NAME));
    }

    #[tokio::test]
    async fn start_installs_producer_and_health() {
        let runtime = KafkaRuntime::default();
        let state = AppState::detached();
        let broker = MemoryBroker::new();
        assert!(!runtime.is_running());

        runtime
            .start(
                &state,
                KafkaConfig::default(),
                &broker,
                vec![],
                &metrics(&[]),
            )
            .unwrap();

        assert!(runtime.is_running());
        let producer = KafkaProducer::from_state(&state).expect("a producer extension");
        producer.send(Record::new("t", "x")).await.unwrap();
        assert!(runtime.producer().is_some());
        assert!(state.health_indicator_registry().contains("kafka"));
        let results = state.health_indicator_registry().run_all().await;
        assert!(matches!(results[0].output.status, HealthStatus::Up));
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn start_rejects_bad_config_and_installs_nothing() {
        let runtime = KafkaRuntime::default();
        let state = AppState::detached();

        let err = runtime
            .start(
                &state,
                KafkaConfig::new(""),
                &MemoryBroker::new(),
                vec![],
                &metrics(&[]),
            )
            .unwrap_err();

        assert!(err.to_string().contains("brokers"), "{err}");
        assert!(!runtime.is_running());
        assert!(KafkaProducer::from_state(&state).is_none());
    }

    #[tokio::test]
    async fn start_rejects_bad_consumers() {
        let runtime = KafkaRuntime::default();
        let consumers = vec![Consumer::new("c", ["t"]).group_id("g")];
        let m = metrics(&consumers);

        let err = runtime
            .start(
                &AppState::detached(),
                KafkaConfig::default(),
                &MemoryBroker::new(),
                consumers,
                &m,
            )
            .unwrap_err();

        assert!(err.to_string().contains("handler"), "{err}");
    }

    #[tokio::test]
    async fn start_twice_is_an_error() {
        let runtime = KafkaRuntime::default();
        let state = AppState::detached();
        let broker = MemoryBroker::new();
        runtime
            .start(
                &state,
                KafkaConfig::default(),
                &broker,
                vec![],
                &metrics(&[]),
            )
            .unwrap();

        let err = runtime
            .start(
                &state,
                KafkaConfig::default(),
                &broker,
                vec![],
                &metrics(&[]),
            )
            .unwrap_err();

        assert!(err.to_string().contains("already"), "{err}");
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn consumers_run_until_shutdown() {
        let runtime = KafkaRuntime::default();
        let broker = MemoryBroker::new();
        let calls = Arc::new(AtomicU32::new(0));
        let consumers = vec![counting(&calls)];
        let m = metrics(&consumers);
        runtime
            .start(
                &AppState::detached(),
                KafkaConfig::default(),
                &broker,
                consumers,
                &m,
            )
            .unwrap();

        broker.publish(Record::new("in", "a"));
        wait_until("one call", || calls.load(Ordering::SeqCst) == 1).await;
        runtime.shutdown().await;
        runtime.shutdown().await; // A second call is safe.
        broker.publish(Record::new("in", "b"));
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(!runtime.is_running());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(broker.committed_offset("g", "in"), Some(1));
    }

    #[tokio::test]
    async fn shutdown_stops_a_stuck_handler_after_the_timeout() {
        let runtime = KafkaRuntime::default();
        let broker = MemoryBroker::new();
        let consumers = vec![
            Consumer::new("c", ["in"])
                .group_id("g")
                .handler(|_, _| futures::future::pending::<Result<(), HandlerError>>()),
        ];
        let m = metrics(&consumers);
        let config = KafkaConfig {
            shutdown_timeout_ms: 100,
            ..KafkaConfig::default()
        };
        runtime
            .start(&AppState::detached(), config, &broker, consumers, &m)
            .unwrap();
        broker.publish(Record::new("in", "x"));
        tokio::time::sleep(Duration::from_millis(50)).await;

        tokio::time::timeout(Duration::from_secs(5), runtime.shutdown())
            .await
            .expect("shutdown ends after the timeout");

        assert_eq!(broker.committed_offset("g", "in"), None);
    }
}
