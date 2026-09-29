# autumn-plugin-kafka

Kafka producer and consumers for [Autumn](https://autumn-web.app) apps.

- Send records from a handler with the `KafkaProducer` argument.
- Process messages with async consumer handlers.
- Retry failed messages. Then send them to a dead-letter topic, or skip them.
- See Kafka status in `/actuator/health` and metrics in `/actuator/prometheus`.
- Test without Docker with `MemoryBroker`.

The example [`examples/orders.rs`](examples/orders.rs) uses all parts.
CI runs its self-check against `MemoryBroker` and a real broker.

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
| `cmake-build` | Build `librdkafka` with CMake. |

## Use

```rust
use autumn_plugin_kafka::{Consumer, HandlerError, KafkaPlugin, KafkaProducer, Message, Record};
use autumn_web::prelude::*;

#[post("/orders/{id}")]
async fn create(producer: KafkaProducer, Path(id): Path<u64>) -> AutumnResult<&'static str> {
    producer.send(Record::new("orders", id.to_string()).with_key(id.to_string())).await?;
    Ok("queued")
}

// The error type can be `HandlerError`, `AutumnError`, or any type with `Display`.
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
It also reads `.env` files and profile aliases (`production`, `development`) as Autumn does.

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
"auto.offset.reset" = "earliest" # default

[kafka.health]
readiness = false                # default: a broker outage does not fail /ready
timeout_ms = 1500                # default
```

- `${NAME}` gets the value of the environment variable `NAME`. If `NAME` is not set, the app does not start. Use `$${` for a literal `${`.
- These variables override the files:
  `AUTUMN_KAFKA__BROKERS`, `__CLIENT_ID`, `__GROUP_ID`, `__SHUTDOWN_TIMEOUT_MS`,
  `__PRODUCER__SEND_TIMEOUT_MS`, `__HEALTH__READINESS`, `__HEALTH__TIMEOUT_MS`.
- Unknown keys are errors. Property values must be strings.
- Config errors name the key. They do not show the value. `Debug` hides secret properties.
- To give the config in code, use `KafkaPlugin::new().config(KafkaConfig::new("broker:9092"))`.

## Delivery rules

- A consumer processes one message at a time, in order.
- The plugin commits an offset only after the handler completes. Delivery is at-least-once.
- A failed handler gets `max_retries` retries (default 3). The delay starts at `retry_backoff` (default 100 ms), doubles for each retry, and stops at 30 s.
- The sum of all retry delays must be less than `max.poll.interval.ms` (default 300 s). If not, the app does not start. Else the broker removes the consumer from the group.
- After the last retry, the plugin sends the message to `dead_letter_topic`, or it logs an error and skips the message.
- A dead-letter record keeps the key, the payload, and the headers. It also gets the `autumn.dlq.*` headers.
- The plugin removes incoming `autumn.dlq.*` headers, so that a producer cannot forge them.
- If the dead-letter send fails, the plugin does not commit. It tries again until shutdown.
- If the broker rejects the dead-letter record (for example, it is too large), the consumer stops with no commit. Health is then `DOWN`. Fix the cause and restart.
- A handler panic is a failure.
- A new group starts at the first message (`auto.offset.reset = earliest`), if you do not set it.
- The producer uses `enable.idempotence = true`, if you do not set it.
- `KafkaProducer::send` times out 500 ms after `producer.send_timeout_ms`. The broker can still get the record after a timeout.

## Health and metrics

The `kafka` health indicator asks a broker for the metadata of all topics.
It is also `DOWN` if a consumer stopped before shutdown.
It is health-only by default. Set `kafka.health.readiness = true` so that a failed check also fails `/ready`.

| Metric | Labels |
|---|---|
| `kafka_messages_produced_total` | |
| `kafka_produce_errors_total` | |
| `kafka_messages_consumed_total` | `consumer` |
| `kafka_handler_errors_total` | `consumer` |
| `kafka_messages_dead_lettered_total` | `consumer` |
| `kafka_messages_skipped_total` | `consumer` |
| `kafka_receive_errors_total` | `consumer` |
| `kafka_consumer_running` (gauge) | `consumer` |

## Test

Use `MemoryBroker` in your app tests. It does not need Docker.
`TestApp` does not run shutdown hooks. Call `runtime.shutdown()` at the end of a test.

```rust
let broker = MemoryBroker::new();
let client = TestApp::new()
    .plugin(KafkaPlugin::new().config(KafkaConfig::default()).backend(broker.clone()))
    .routes(routes![create])
    .build();

client.post("/orders/7").send().await.assert_status(200);
assert_eq!(broker.messages("orders").len(), 1);
```

To test a handler alone, call it with `Message::new(..)` and `AppState::detached()`.
To test code that takes a producer, use `KafkaProducer::from_backend`.
`MemoryBroker::set_available(false)` and `MemoryBroker::reject_topic(..)` simulate failures.

To run the tests of this crate against a real broker:

```sh
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1
KAFKA_BROKERS=localhost:9092 cargo test
```

Without `KAFKA_BROKERS`, the broker tests do nothing. In CI, they fail.

## License

Apache-2.0
