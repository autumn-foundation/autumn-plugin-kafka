//! Tests for `RdKafkaBackend` against a real broker.
//!
//! Set `KAFKA_BROKERS` (for example `localhost:9092`) to run them.

mod common;

use std::time::Duration;

use autumn_plugin_kafka::{
    Backend, ConsumerBackend, ConsumerSpec, KafkaConfig, Message, RdKafkaBackend, Record,
};

const WAIT: Duration = Duration::from_secs(30);

fn spec(group: &str, topic: &str) -> ConsumerSpec {
    ConsumerSpec::new(group, group, [topic]).with_property("auto.offset.reset", "earliest")
}

async fn recv(consumer: &mut Box<dyn ConsumerBackend>) -> Message {
    tokio::time::timeout(WAIT, consumer.recv())
        .await
        .expect("a message in time")
        .expect("no receive error")
}

#[tokio::test(flavor = "multi_thread")]
async fn round_trip_keeps_key_payload_and_headers() {
    let Some(brokers) = common::brokers("round_trip") else {
        return;
    };
    let config = KafkaConfig::new(brokers);
    let topic = common::unique("rt");
    let producer = RdKafkaBackend.producer(&config).unwrap();

    producer.ping(WAIT).await.unwrap();
    let delivery = producer
        .send(
            Record::new(&topic, "payload")
                .with_key("k")
                .with_header("h", "v"),
            WAIT,
        )
        .await
        .unwrap();
    producer.flush(WAIT).await.unwrap();

    let mut consumer = RdKafkaBackend
        .consumer(&config, &spec(&common::unique("g"), &topic))
        .unwrap();
    let msg = recv(&mut consumer).await;
    assert_eq!(msg.topic(), topic);
    assert_eq!(msg.partition(), delivery.partition);
    assert_eq!(msg.offset(), delivery.offset);
    assert_eq!(msg.key(), Some(&b"k"[..]));
    assert_eq!(msg.payload(), b"payload");
    assert_eq!(msg.header("h"), Some(&b"v"[..]));
    assert!(msg.timestamp_ms().is_some());
    consumer.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tombstone_round_trip() {
    let Some(brokers) = common::brokers("tombstone_round_trip") else {
        return;
    };
    let config = KafkaConfig::new(brokers);
    let topic = common::unique("ts");
    let producer = RdKafkaBackend.producer(&config).unwrap();
    producer
        .send(Record::tombstone(&topic, "k"), WAIT)
        .await
        .unwrap();

    let mut consumer = RdKafkaBackend
        .consumer(&config, &spec(&common::unique("g"), &topic))
        .unwrap();
    let msg = recv(&mut consumer).await;
    assert!(msg.is_tombstone());
    assert_eq!(msg.key(), Some(&b"k"[..]));
    consumer.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn new_consumer_resumes_after_last_commit() {
    let Some(brokers) = common::brokers("resume") else {
        return;
    };
    let config = KafkaConfig::new(brokers);
    let topic = common::unique("resume");
    let group = common::unique("g");
    let producer = RdKafkaBackend.producer(&config).unwrap();
    for payload in ["0", "1", "2"] {
        producer
            .send(Record::new(&topic, payload), WAIT)
            .await
            .unwrap();
    }

    let mut first = RdKafkaBackend
        .consumer(&config, &spec(&group, &topic))
        .unwrap();
    let m0 = recv(&mut first).await;
    assert_eq!(m0.payload(), b"0");
    first.commit(&m0).unwrap();
    let m1 = recv(&mut first).await;
    assert_eq!(m1.payload(), b"1"); // Not committed.
    first.close().await;

    let mut second = RdKafkaBackend
        .consumer(&config, &spec(&group, &topic))
        .unwrap();
    assert_eq!(recv(&mut second).await.payload(), b"1");
    second.close().await;
}
