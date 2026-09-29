//! Tests for `KafkaPlugin` inside an Autumn test app.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_plugin_kafka::{
    Consumer, HandlerError, KafkaConfig, KafkaPlugin, KafkaProducer, MemoryBroker, Message, Record,
};
use autumn_web::prelude::*;
use autumn_web::test::TestApp;

#[post("/orders")]
async fn create_order(producer: KafkaProducer) -> AutumnResult<&'static str> {
    producer
        .send(Record::new("orders", r#"{"id":1}"#).key("1"))
        .await?;
    Ok("sent")
}

type Seen = Arc<Mutex<Vec<String>>>;

fn recorder(topic: &str, group: &str, seen: &Seen) -> Consumer {
    let seen = Arc::clone(seen);
    Consumer::new("recorder", [topic])
        .group_id(group)
        .property("auto.offset.reset", "earliest")
        .handler(move |msg: Message, _state| {
            seen.lock()
                .unwrap()
                .push(String::from_utf8_lossy(msg.payload()).into_owned());
            async { Ok::<_, HandlerError>(()) }
        })
}

async fn wait_until(what: &str, check: impl Fn() -> bool) {
    for _ in 0..3000 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out: {what}");
}

fn memory_plugin(broker: &MemoryBroker) -> KafkaPlugin {
    KafkaPlugin::new()
        .config(KafkaConfig::default())
        .backend(broker.clone())
}

#[tokio::test(flavor = "multi_thread")]
async fn route_sends_with_the_producer_extractor() {
    let broker = MemoryBroker::new();
    let client = TestApp::new()
        .plugin(memory_plugin(&broker))
        .routes(routes![create_order])
        .build();

    client
        .post("/orders")
        .send()
        .await
        .assert_status(200)
        .assert_body_contains("sent");

    let sent = broker.messages("orders");
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].key(), Some(&b"1"[..]));
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_gets_messages() {
    let broker = MemoryBroker::new();
    let seen: Seen = Arc::default();
    let plugin = memory_plugin(&broker).consumer(recorder("events", "g", &seen));
    let runtime = plugin.runtime();
    let _client = TestApp::new().plugin(plugin).build();

    broker.publish(Record::new("events", "hello"));

    wait_until("one message", || seen.lock().unwrap().len() == 1).await;
    assert_eq!(seen.lock().unwrap()[0], "hello");
    runtime.shutdown().await;
    assert_eq!(broker.committed_offset("g", "events"), Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn health_endpoint_reports_kafka() {
    let broker = MemoryBroker::new();
    let client = TestApp::new().plugin(memory_plugin(&broker)).build();

    let up = client.get("/actuator/health").send().await;
    up.assert_status(200);
    let body: serde_json::Value = up.json();
    assert_eq!(body["components"]["kafka"]["status"], "UP");

    broker.set_available(false);
    let down = client.get("/actuator/health").send().await;
    let body: serde_json::Value = down.json();
    assert_eq!(body["components"]["kafka"]["status"], "DOWN");
}

#[tokio::test(flavor = "multi_thread")]
async fn prometheus_endpoint_shows_kafka_counters() {
    let broker = MemoryBroker::new();
    let client = TestApp::new()
        .plugin(memory_plugin(&broker))
        .routes(routes![create_order])
        .build();
    client.post("/orders").send().await.assert_status(200);

    client
        .get("/actuator/prometheus")
        .send()
        .await
        .assert_status(200)
        .assert_body_contains("kafka_messages_produced_total 1");
}

#[tokio::test(flavor = "multi_thread")]
async fn end_to_end_with_a_real_broker() {
    let Some(brokers) = common::brokers("end_to_end_with_a_real_broker") else {
        return;
    };
    let topic = common::unique("e2e");
    let seen: Seen = Arc::default();
    let plugin = KafkaPlugin::new()
        .config(KafkaConfig::new(brokers))
        .consumer(recorder(&topic, &common::unique("g"), &seen));
    let runtime = plugin.runtime();
    let _client = TestApp::new().plugin(plugin).build();

    let producer = runtime.producer().expect("a producer");
    producer.send(Record::new(&topic, "real")).await.unwrap();

    wait_until("one message", || seen.lock().unwrap().len() == 1).await;
    assert_eq!(seen.lock().unwrap()[0], "real");
    runtime.shutdown().await;
}
