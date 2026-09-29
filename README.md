# autumn-plugin-kafka

Kafka producer and consumers for [Autumn](https://autumn-web.app) apps.

- Send records from a handler with the `KafkaProducer` argument.
- Process messages with async consumer handlers.
- Retry failed messages. Then send them to a dead-letter topic, or skip them.
- See Kafka status in `/actuator/health` and counters in `/actuator/prometheus`.
- Test without Docker with `MemoryBroker`.

## Install

```toml
[dependencies]
autumn-plugin-kafka = "0.1"
```

The crate compiles `librdkafka`. You must have a C compiler, `make`, and `zlib`.
For TLS or SASL, enable the `ssl` feature.

| Feature | Function |
|---|---|
| `ssl` | TLS and SASL (PLAIN, SCRAM) with the system OpenSSL. |
| `ssl-vendored` | The same as `ssl`, with a built-in OpenSSL. |
| `gssapi` | SASL GSSAPI (Kerberos). |
| `zstd` | `zstd` compression. |
| `dynamic-linking` | Use the system `librdkafka`. |

## Use

```rust
use autumn_plugin_kafka::{Consumer, HandlerError, KafkaPlugin, KafkaProducer, Message, Record};
use autumn_web::prelude::*;

#[post("/orders/{id}")]
async fn create(producer: KafkaProducer, Path(id): Path<u64>) -> AutumnResult<&'static str> {
    producer.send(Record::new("orders", id.to_string()).key(id.to_string())).await?;
    Ok("queued")
}

async fn on_order(msg: Message, _state: AppState) -> Result<(), HandlerError> {
    tracing::info!(payload = ?msg.payload(), "order received");
    Ok(())
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            KafkaPlugin::new().consumer(
                Consumer::new("orders", ["orders"])
                    .group_id("billing")
                    .handler(on_order)
                    .max_retries(5)
                    .dead_letter_topic("orders.dlq"),
            ),
        )
        .routes(routes![create])
        .run()
        .await;
}
```

## Configure

The plugin reads `[kafka]` at startup. It uses the same layers as Autumn:
`autumn.toml`, `[profile.<name>.kafka]`, `autumn-<profile>.toml`, then environment variables.

```toml
[kafka]
brokers = "localhost:9092"       # default
client_id = "autumn"             # default
group_id = "billing"             # default group for consumers
shutdown_timeout_ms = 10000      # default

[kafka.properties]               # librdkafka properties for all clients
"security.protocol" = "SASL_SSL"
"sasl.mechanisms" = "PLAIN"
"sasl.username" = "${KAFKA_USER}"
"sasl.password" = "${KAFKA_PASSWORD}"

[kafka.producer]
send_timeout_ms = 5000           # default
properties = { "linger.ms" = "5" }

[kafka.consumer.properties]
"auto.offset.reset" = "earliest"

[kafka.health]
readiness = false                # default: a broker outage does not fail /ready
timeout_ms = 1500                # default
```

- `${NAME}` gets the value of the environment variable `NAME`. If `NAME` is not set, the app does not start.
- `AUTUMN_KAFKA__BROKERS`, `AUTUMN_KAFKA__CLIENT_ID`, and `AUTUMN_KAFKA__GROUP_ID` override the file.
- Unknown keys are errors.
- To give the config in code, use `KafkaPlugin::new().config(KafkaConfig::new("broker:9092"))`.

## Delivery rules

- A consumer processes one message at a time, in order.
- The plugin commits an offset only after the handler completes. Delivery is at-least-once.
- A failed handler gets `max_retries` retries (default 3). The delay starts at `retry_backoff` (default 100 ms), doubles for each retry, and stops at 30 s.
- After the last retry, the plugin sends the message to `dead_letter_topic`, or it logs an error and skips the message.
- A dead-letter record keeps the key, the payload, and the headers. It also gets the `autumn.dlq.*` headers.
- If the dead-letter send fails, the plugin does not commit. It tries again until shutdown.
- A handler panic is a failure.
- The producer uses `enable.idempotence = true`, if you do not set it.

## Health and metrics

The `kafka` health indicator asks a broker for metadata.
It is health-only by default. Set `kafka.health.readiness = true` to also gate `/ready`.

| Counter | Labels |
|---|---|
| `kafka_messages_produced_total` | |
| `kafka_produce_errors_total` | |
| `kafka_messages_consumed_total` | `consumer` |
| `kafka_handler_errors_total` | `consumer` |
| `kafka_messages_dead_lettered_total` | `consumer` |
| `kafka_messages_skipped_total` | `consumer` |
| `kafka_receive_errors_total` | `consumer` |

## Test

Use `MemoryBroker` in your app tests. It does not need Docker.

```rust
let broker = MemoryBroker::new();
let client = TestApp::new()
    .plugin(KafkaPlugin::new().config(KafkaConfig::default()).backend(broker.clone()))
    .routes(routes![create])
    .build();

client.post("/orders/7").send().await.assert_status(200);
assert_eq!(broker.messages("orders").len(), 1);
```

To run the tests of this crate against a real broker:

```sh
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1
KAFKA_BROKERS=localhost:9092 cargo test
```

Without `KAFKA_BROKERS`, the broker tests do nothing.

## License

Apache-2.0
