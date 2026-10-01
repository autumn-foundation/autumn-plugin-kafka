# Planning record

This file records the planning for `autumn-plugin-kafka`.
The text uses ASD-STE100 (Simplified Technical English).

## 1. Goal

Give an Autumn application a Kafka producer and Kafka consumers.
Install the plugin with one line.

## 2. Facts (from `autumn-web` 0.7.0)

- A plugin implements `Plugin::build(self, AppBuilder) -> AppBuilder`.
- Autumn loads config after `build`. Because of this, the plugin reads `[kafka]` in a startup hook.
- `AppBuilder::config_section("kafka")` makes `[kafka]` safe for `strict_config`.
- `on_startup` gets `AppState`. `on_shutdown` runs in reverse order.
- `health_indicator` and `metrics_source` add to `/actuator/*`.
- `MetricsSource::collect` must not do I/O.
- `AppBuilder::health_indicator` reads the group at build time. Because of this, the startup hook registers the indicator on `AppState`, after it reads `[kafka]`.
- `TestApp` runs startup hooks with `Handle::block_on`. It does not run shutdown hooks.
- Config layers: `autumn.toml`, `[profile.<name>]`, `autumn-<profile>.toml`, `AUTUMN_*` env.
- Third-party plugin crates use the name `autumn-plugin-<name>`.
- `rdkafka` is the mature client. `autumn-harvest-plugin` also uses it.
- `rskafka` has no consumer groups. Because of this, we do not use it.

## 3. Brainstorming

We wrote all ideas first. Then we sorted them.

| Idea | Decision |
|---|---|
| Producer handle as a request extractor | Yes |
| Consumers with an async handler | Yes |
| Handler gets `AppState` (for `Db`, config, extensions) | Yes |
| `[kafka]` config with profile and env layers | Yes |
| `${VAR}` values for secrets | Yes |
| Pass-through `librdkafka` properties (SASL, TLS) | Yes |
| Health indicator (broker metadata probe) | Yes |
| Metrics source (counters per consumer) | Yes |
| Retries with backoff, then dead-letter topic | Yes |
| Graceful shutdown (stop consumers, flush producer) | Yes |
| In-memory broker for tests without Docker | Yes |
| JSON helpers on records and messages | Yes |
| Transactions and exactly-once | No (later) |
| Schema registry and Avro | No (later) |
| Outbox pattern | No (later) |
| `#[kafka_listener]` macro | No (later) |
| Parallel handlers in one consumer | No. Order is more important. |

## 4. Reverse brainstorming

Question: "How can we make this plugin fail?" Each answer gives a rule.

| How to fail | Rule |
|---|---|
| Commit the offset before the handler completes | Store the offset only after success. |
| A poison message blocks the consumer forever | Retry a limited number of times. Then dead-letter or skip. |
| Skip a message with no signal | Log an error. Increment a counter. |
| Lose a message when the dead-letter send fails | Do not commit. Retry the send until shutdown. |
| A handler panic stops the consumer | Catch the panic. Count it as a failure. |
| Block the async runtime | Run blocking `librdkafka` calls in `spawn_blocking`. |
| Stop the app boot when the broker is down | Do not connect at startup. Show the broker state in the health check. |
| A broker outage removes all replicas from the load balancer | The health indicator is health-only by default. |
| Shutdown does not stop | Use a shutdown timeout. Then abort the tasks. |
| Show passwords in logs | Redact secret properties in `Debug`. |
| Accept a config typo with no error | Deny unknown fields. Validate at startup. |
| Two consumers share a name | Reject at startup. |
| A consumer has no group id | Reject at startup. |
| Tests need Docker | Supply `MemoryBroker`. |
| The health check stops the health endpoint | Use a probe timeout that is less than the indicator timeout. |

## 5. Six thinking hats

- **White (facts):** See section 2.
- **Red (feelings):** Users want one line to install. Users fear message loss. The native build is slow.
- **Black (risks):** See section 4. Also: integration tests need a broker, so CI must start one.
- **Yellow (benefits):** One health endpoint, one metrics endpoint, and one config file for all parts.
- **Green (new ideas):** Tests can use `MemoryBroker`. Handlers get `AppState`. Dead-letter headers keep the source position.
- **Blue (process):** Use red, green, refactor for each unit. Commit each phase. Then do a review with agents.

## 6. Design

```text
KafkaPlugin ──build──▶ config_section("kafka")
                       metrics_source("kafka")
                       on_startup ─▶ load config ─▶ Backend ─▶ KafkaProducer (extension)
                                                          ├──▶ health indicator "kafka"
                                                          └──▶ consumer tasks
                       on_shutdown ─▶ KafkaRuntime::shutdown
```

- `Backend` makes producers and consumers. `RdKafkaBackend` is the default. `MemoryBroker` is for tests.
- One task for each consumer. The task does one message at a time.
- Delivery is at-least-once.

## 7. TDD cycles

Each cycle has a red commit, a green commit, and (if necessary) a refactor commit.

1. `KafkaConfig`: parse, layers, env, `${VAR}`, validate, redact.
2. `Record` and `Message`: builders, JSON, headers.
3. `MemoryBroker`: publish, receive, commit, resume, outage.
4. `KafkaProducer`: send, metrics, extractor.
5. Consumer loop: success, retry, skip, dead-letter, panic, shutdown.
6. `KafkaHealth`: up, down, timeout, group.
7. `KafkaMetrics`: families and labels.
8. `RdKafkaBackend`: tests against a real broker.
9. `KafkaPlugin`: wire all parts. Test with `TestApp`.

We did cycle 8 before cycle 9, because the plugin uses `RdKafkaBackend` as the default.
Clippy found one bug that the tests did not find (the panic text was lost).
We added a red test for it first. Then we fixed it.

## 8. Code review

Five agents did a review. Each agent had one angle:

| Angle | Main findings that we fixed |
|---|---|
| Kafka semantics | A rejected dead-letter send blocked a partition. `auto.offset.reset` was `latest`. Retry delays could exceed `max.poll.interval.ms`. |
| Async safety | A dropped `shutdown` lost task handles. An aborted task closed the client on a runtime thread. |
| Security | A header key that is not UTF-8 made `rdkafka` panic. Some errors and `Debug` output showed secrets. Dead-letter headers could be forged. |
| API and docs | Handlers could not return `AutumnResult`. `.env` and profile aliases did not work. Names were not the same on `Record` and `Message`. |
| Tests | 16 of 24 mutations were not found. We added tests for shutdown, flush, backoff, and dead-letter edge cases. |

For each fix, we wrote a red test first. Then we made it green.
