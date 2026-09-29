//! An order service that uses all parts of `autumn-plugin-kafka`.
//!
//! | Part | Where |
//! |---|---|
//! | `KafkaProducer` handler argument, JSON records, keys, headers | `place_order` |
//! | Tombstones | `cancel_order` |
//! | Consumer with `AppState`, `AutumnResult`, retries, dead-letter topic | `billing` |
//! | A second group on the same topic (fan-out), a consumer property | `audit` |
//! | Dead-letter headers | `dead_letter_watch` |
//! | `[kafka]` config, health, metrics, shutdown | `main` and `self_check` |
//! | `MemoryBroker` | `self_check --memory` |
//!
//! Start a broker and the server:
//!
//! ```sh
//! docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1
//! cargo run --example orders
//! ```
//!
//! Send requests:
//!
//! ```sh
//! curl -X POST localhost:3000/orders -H 'content-type: application/json' \
//!      -d '{"id": 1, "item": "tea", "cents": 450}'
//! curl -X POST localhost:3000/orders -H 'content-type: application/json' \
//!      -d '{"id": 2, "item": "free lunch", "cents": -1}'   # goes to the dead-letter topic
//! curl -X POST localhost:3000/orders/1/cancel               # sends a tombstone
//! curl localhost:3000/ledger
//! curl localhost:3000/actuator/health
//! curl -s localhost:3000/actuator/prometheus | grep kafka_
//! ```
//!
//! Run the self-check (CI runs both):
//!
//! ```sh
//! KAFKA_BROKERS=localhost:9092 cargo run --example orders -- --check
//! cargo run --example orders -- --check --memory
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use autumn_plugin_kafka::{
    Consumer, DLQ_HEADER_ERROR, DLQ_HEADER_OFFSET, DLQ_HEADER_TOPIC, KafkaConfig, KafkaPlugin,
    KafkaProducer, MemoryBroker, Message, Record,
};
use autumn_web::prelude::*;
use autumn_web::test::{TestApp, TestClient};
use serde::{Deserialize, Serialize};

const ORDERS: &str = "orders.placed";
const ORDERS_DLQ: &str = "orders.placed.dlq";

// ── Domain ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: u64,
    item: String,
    cents: i64,
}

#[derive(Serialize, Deserialize)]
struct Placed {
    partition: i32,
    offset: i64,
}

/// What the consumers saw. It is an app extension, so handlers get it from `AppState`.
#[derive(Default)]
struct Ledger(Mutex<LedgerView>);

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct LedgerView {
    billed: BTreeMap<u64, Order>,
    cancelled: BTreeSet<u64>,
    dead_letters: Vec<DeadLetter>,
    audit_events: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeadLetter {
    key: String,
    source_topic: String,
    source_offset: String,
    error: String,
}

impl Ledger {
    fn get(state: &AppState) -> AutumnResult<Arc<Self>> {
        state
            .extension::<Self>()
            .ok_or_else(|| AutumnError::internal_server_error_msg("no ledger"))
    }

    fn update(&self, change: impl FnOnce(&mut LedgerView)) {
        change(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner));
    }

    fn view(&self) -> LedgerView {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

// ── HTTP routes: the producer side ──────────────────────────────────────────

/// Sends the order as JSON. The key keeps all events of one order in one partition.
#[post("/orders")]
async fn place_order(
    producer: KafkaProducer,
    Json(order): Json<Order>,
) -> AutumnResult<Json<Placed>> {
    let record = Record::json(ORDERS, &order)?
        .with_key(order.id.to_string())
        .with_header("source", "http");
    let delivery = producer.send(record).await?;
    Ok(Json(Placed {
        partition: delivery.partition,
        offset: delivery.offset,
    }))
}

/// Sends a tombstone: a record with a key and no payload.
#[post("/orders/{id}/cancel")]
async fn cancel_order(producer: KafkaProducer, Path(id): Path<u64>) -> AutumnResult<&'static str> {
    producer
        .send(Record::tombstone(ORDERS, id.to_string()))
        .await?;
    Ok("cancelled")
}

#[get("/ledger")]
async fn ledger(State(state): State<AppState>) -> AutumnResult<Json<LedgerView>> {
    Ok(Json(Ledger::get(&state)?.view()))
}

// ── Consumers ───────────────────────────────────────────────────────────────

/// Bills an order. A bad order fails. After the retries, the plugin sends it
/// to the dead-letter topic.
async fn bill(msg: Message, state: AppState) -> AutumnResult<()> {
    let ledger = Ledger::get(&state)?;
    let key = String::from_utf8_lossy(msg.key().unwrap_or_default()).into_owned();
    if msg.is_tombstone() {
        let id = key
            .parse()
            .map_err(|_| AutumnError::bad_request_msg("bad key"))?;
        ledger.update(|v| {
            v.billed.remove(&id);
            v.cancelled.insert(id);
        });
        return Ok(());
    }
    let order: Order = msg.json()?;
    if order.cents <= 0 {
        return Err(AutumnError::bad_request_msg(format!(
            "order {} has no price",
            order.id
        )));
    }
    tracing::info!(id = order.id, source = ?msg.header("source"), "order billed");
    ledger.update(|v| {
        v.billed.insert(order.id, order);
    });
    Ok(())
}

fn billing() -> Consumer {
    Consumer::new("billing", [ORDERS])
        .group_id("billing")
        .handler(bill)
        .max_retries(2)
        .retry_backoff(Duration::from_millis(50))
        .dead_letter_topic(ORDERS_DLQ)
}

/// A second group on the same topic gets every message too.
fn audit() -> Consumer {
    Consumer::new("audit", [ORDERS])
        .group_id("audit")
        .property("fetch.wait.max.ms", "100")
        .handler(|_msg: Message, state: AppState| async move {
            Ledger::get(&state)?.update(|v| v.audit_events += 1);
            Ok::<_, AutumnError>(())
        })
}

/// Reads the dead-letter topic and the `autumn.dlq.*` headers.
fn dead_letter_watch() -> Consumer {
    Consumer::new("dlq-watch", [ORDERS_DLQ])
        .group_id("dlq-watch")
        .handler(|msg: Message, state: AppState| async move {
            let text =
                |name| String::from_utf8_lossy(msg.header(name).unwrap_or_default()).into_owned();
            let dead = DeadLetter {
                key: String::from_utf8_lossy(msg.key().unwrap_or_default()).into_owned(),
                source_topic: text(DLQ_HEADER_TOPIC),
                source_offset: text(DLQ_HEADER_OFFSET),
                error: text(DLQ_HEADER_ERROR),
            };
            tracing::warn!(?dead, "order in the dead-letter topic");
            Ledger::get(&state)?.update(|v| v.dead_letters.push(dead));
            Ok::<_, AutumnError>(())
        })
}

fn plugin() -> KafkaPlugin {
    KafkaPlugin::new()
        .consumer(billing())
        .consumer(audit())
        .consumer(dead_letter_watch())
}

// ── Main ────────────────────────────────────────────────────────────────────

#[autumn_web::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--check") {
        self_check(args.iter().any(|a| a == "--memory")).await;
        return;
    }
    // The plugin reads `[kafka]` from `autumn.toml` and `AUTUMN_KAFKA__*`.
    autumn_web::app()
        .plugin(plugin())
        .state_initializer(|state| state.insert_extension(Ledger::default()))
        .routes(routes![place_order, cancel_order, ledger])
        .run()
        .await;
}

// ── Self-check: drives the same app and checks every part ───────────────────

async fn self_check(memory: bool) {
    let plugin = if memory {
        plugin()
            .config(KafkaConfig::default())
            .backend(MemoryBroker::new())
    } else {
        let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
        plugin().config(KafkaConfig::new(brokers))
    };
    let runtime = plugin.runtime();
    let client = TestApp::new()
        .plugin(plugin)
        .state_initializer(|state| state.insert_extension(Ledger::default()))
        .routes(routes![place_order, cancel_order, ledger])
        .build();

    // Order IDs that no earlier run used.
    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs()
        * 1000;
    let (paid, cancelled, poison) = (base + 1, base + 2, base + 3);

    place(&client, paid, 450).await;
    place(&client, cancelled, 300).await;
    place(&client, poison, -1).await;
    client
        .post(&format!("/orders/{cancelled}/cancel"))
        .send()
        .await
        .assert_status(200);

    let view = wait_for(&client, |v| {
        v.billed.contains_key(&paid)
            && v.cancelled.contains(&cancelled)
            && v.dead_letters.iter().any(|d| d.key == poison.to_string())
            && v.audit_events >= 4
    })
    .await;
    let dead = view
        .dead_letters
        .iter()
        .find(|d| d.key == poison.to_string())
        .expect("the poison order");
    assert_eq!(dead.source_topic, ORDERS);
    assert!(dead.error.contains("has no price"), "{dead:?}");
    assert!(!view.billed.contains_key(&cancelled));

    let health = client.get("/actuator/health").send().await;
    let health: serde_json::Value = health.json();
    assert_eq!(health["components"]["kafka"]["status"], "UP", "{health}");

    let metrics = client.get("/actuator/prometheus").send().await;
    for line in [
        "kafka_consumer_running{consumer=\"billing\"} 1",
        "kafka_consumer_running{consumer=\"dlq-watch\"} 1",
        "kafka_messages_dead_lettered_total{consumer=\"billing\"}",
        "kafka_handler_errors_total{consumer=\"billing\"}",
    ] {
        metrics.assert_body_contains(line);
    }

    runtime.shutdown().await;
    assert!(!runtime.is_running());
    println!(
        "orders example: self-check passed ({} backend)",
        if memory { "memory" } else { "Kafka" }
    );
}

async fn place(client: &TestClient, id: u64, cents: i64) {
    let order = Order {
        id,
        item: "tea".into(),
        cents,
    };
    client
        .post("/orders")
        .json(&serde_json::to_value(&order).expect("order is JSON"))
        .send()
        .await
        .assert_status(200);
}

async fn wait_for(client: &TestClient, done: impl Fn(&LedgerView) -> bool) -> LedgerView {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let view: LedgerView = client.get("/ledger").send().await.json();
        if done(&view) {
            return view;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out; ledger: {view:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
