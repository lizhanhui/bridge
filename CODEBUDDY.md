# CODEBUDDY.md

This file provides guidance to CodeBuddy Code when working with code in this repository.

## Overview

Early-stage Java (Maven) service that bridges MQTT brokers and RocketMQ: it consumes messages from a source, applies a SQL (PartiQL) transformation to the payload, and publishes to a sink. Source and sink can each be MQTT or RocketMQ, so data can flow in either direction.

## Build & Run

- Build: `mvn compile` (or `mvn package`)
- Entry point: `com.tencent.cloud.mqtt.TaskManager` — loads `conf/tasks.json` (or path from `args[0]`), builds connectors/tasks, runs each `Task` on its own virtual thread, with a shutdown hook that interrupts task threads
- Tests: `mvn test` (JUnit 5 + Surefire); run a single test with `mvn test -Dtest=SQLTransformTest`
- No lint tooling is configured yet
- Compiler target is Java 25 — ensure the JDK matches before building
- Run configuration is defined in `conf/tasks.json` (connectors + tasks)

## Architecture

Data flow: **Source → Transform → Sink**, orchestrated per `Task`.

- `com.tencent.cloud.mqtt` — core abstractions:
  - `Source`, `Sink` — marker interfaces (no methods yet)
  - `Transform<K, V>` — `Optional<List<Record<K, V>>> transform(Record<K, V>)` using Kafka Streams' `org.apache.kafka.streams.processor.api.Record`; empty = record filtered out
  - `Task` — a named Source/Transform/Sink triple with a `launch()` stub
  - `TaskManager` — main entry point; loads `conf/tasks.json`, builds connectors and tasks
  - `SQLTransform<K, V>` — Transform implementation using PartiQL (`partiql-lang-kotlin`) over JSON payloads. The query is compiled once in the constructor and reused; JSON is parsed via ion-java (Ion is a superset of JSON); the record value (must be JSON text) is bound to the global name `payload`, so SQL in task configs reads `SELECT ... FROM payload WHERE ...`. Malformed JSON payloads are dropped (logged) and yield empty. Output records reuse the input key/timestamp/headers with each result row serialized back to JSON.
- `com.tencent.cloud.mqtt.model` — config model POJOs mirroring `conf/tasks.json`:
  - `Connector` — connection credentials; `ConnectorType` enum is `MQTT` / `RocketMQ`
  - `MqttSource` (topic filter, supports MQTT shared subscriptions like `$share/group/...`), `MqttSink` (topic)
  - `RocketMQSource` (consumer group + topic list), `RocketMQSink` (topic)

`conf/tasks.json` schema: a `connectors` array (id/type/access_point/username/password) and a `tasks` array; each task references connectors by `connector_id` and carries a `sql` PartiQL statement executed against the incoming payload.

## Key dependencies

- `hivemq-mqtt-client` — MQTT connectivity
- `rocketmq-client` (5.5.0) — RocketMQ connectivity
- `kafka-streams` (4.3.1) — only for its `processor.api.Record` type used as the transform input/output; no Kafka brokers are involved
- `partiql-lang-kotlin` — SQL-over-JSON transformation engine
- `kotlin-stdlib` (1.6.20) — required by PartiQL at runtime
- `jackson-databind` — parsing `conf/tasks.json`
- `slf4j-api` + `slf4j-simple` (runtime binding) — logging
