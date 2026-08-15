# CODEBUDDY.md

This file provides guidance to CodeBuddy Code when working with code in this repository.

## Overview

Early-stage Java (Maven) service that bridges MQTT brokers and RocketMQ: it consumes messages from a source, applies a SQL (PartiQL) transformation to the payload, and publishes to a sink. Source and sink can each be MQTT or RocketMQ, so data can flow in either direction.

## Build & Run

- Build: `mvn compile` (or `mvn package` — the shade plugin produces a runnable fat jar `target/bridge-1.0-SNAPSHOT.jar` with `TaskManager` as the manifest main class; run with `java -jar`)
- Entry point: `com.tencent.cloud.mqtt.TaskManager` — loads `conf/tasks.json` (or path from `args[0]`), builds connectors/tasks, runs each `Task` on its own virtual thread, with a shutdown hook that interrupts task threads
- Tests: `mvn test` (JUnit 5 + Surefire); run a single test with `mvn test -Dtest=SQLTransformTest`
- No lint tooling is configured yet
- Compiler target is Java 25 — ensure the JDK matches before building
- Run configuration is defined in `conf/tasks.json` (connectors + tasks)

## Architecture

Data flow: **Source → Transform → Sink**, orchestrated per `Task`.

- `com.tencent.cloud.mqtt` — core abstractions:
  - `Source` — `AckableRecord poll()` (blocks; null when closed and drained); `Sink` — `void publish(Record)`. Source errors fail-fast and kill the task lane; sink errors propagate to `Task`, which retries recoverable failures with exponential backoff (1s→60s, indefinite) and skips poison (`IllegalArgumentException`) records with a warning + ack.
  - `AckableRecord` — abstract `Record<String, String>` subclass carrying the source's ack context; `ack()` is idempotent and delegates to `doAck()`. Crash-safety contract: `Task` acks only after processing completes (filtered out, max_hops drop, poison-skip, or all results published), so an unacked record is redelivered by the broker. Each source provides a subtype (`mqtt.MqttAckableRecord` holds the `Mqtt5Publish`; `rocketmq.RocketMQAckableRecord` holds the `SimpleConsumer` + `MessageView`).
  - `Transform<K, V>` — `Optional<List<Record<K, V>>> transform(Record<K, V>)` using Kafka Streams' `org.apache.kafka.streams.processor.api.Record`; empty = record filtered out
  - `Task` — a named Source/Transform/Sink triple with a `launch()` loop; enforces loop prevention via the `bridge-hop-count` record header (travels as an MQTT/RocketMQ user property): incremented on every pass, records arriving with a count greater than the task's `max_hops` (default 1) are skipped (which also acks them)
  - `TaskManager` — main entry point; loads `conf/tasks.json`, expands each task into `parallelism` lane(s) named `<name>-<seq>`, builds connectors and tasks
  - `SQLTransform<K, V>` — Transform implementation using PartiQL (`partiql-lang-kotlin`) over JSON payloads. The query is compiled once in the constructor and reused; JSON is parsed via ion-java (Ion is a superset of JSON); the record value (must be JSON text) is bound to the global name `payload`, so SQL in task configs reads `SELECT ... FROM payload WHERE ...`. Malformed JSON payloads are dropped (logged) and yield empty. Output records reuse the input key/timestamp/headers with each result row serialized back to JSON.
- `com.tencent.cloud.mqtt.model` — config model POJOs mirroring `conf/tasks.json`:
  - `Connector` — connection credentials; `ConnectorType` enum is `MQTT` / `RocketMQ`
- `com.tencent.cloud.mqtt.mqtt` — `MqttSource` (topic filter, supports MQTT shared subscriptions like `$share/group/...`; hivemq manual ack, persistent session 24h), `MqttSink` (topic), `MqttRecordMapper`, `MqttClients`
- `com.tencent.cloud.mqtt.rocketmq` — `RocketMQSource` (RocketMQ 5.x SimpleConsumer, one message per RPC with 30s invisible duration; ack only after processing → broker redelivers unacked messages), `RocketMQSink` (blocking Producer), `RocketMQRecordMapper` (plain mapping: properties ↔ same-name headers, `rmq.*` reserved headers, record key = broker message ID), `RocketMQClients`

`conf/tasks.json` schema: a `connectors` array (id/type/access_point/username/password) and a `tasks` array; each task references connectors by `connector_id`, carries a `sql` PartiQL statement executed against the incoming payload, and may set `max_hops` (loop-prevention limit, default 1). Every task runs as one or more independent lanes named `<name>-<seq>` (seq from 0), each with its own source/transform/sink and clients; `parallelism` (optional int ≥ 1, default 1) sets the lane count. MQTT source lanes need a `$share/` filter to split load, otherwise every lane receives every message; RocketMQ source lanes share the task's `consumer_group` and load-balance natively. Source configs: MQTT = `topic_filter`; RocketMQ = `consumer_group` + `topics` array. Sink configs: `topic` (both types). RocketMQ `access_point` is the 5.x proxy `host:port`.

## Key dependencies

- `hivemq-mqtt-client` — MQTT connectivity
- `rocketmq-client-java` (5.2.1) — RocketMQ connectivity (5.x proxy protocol: SimpleConsumer + Producer)
- `kafka-streams` (4.3.1) — only for its `processor.api.Record` type used as the transform input/output; no Kafka brokers are involved
- `partiql-lang-kotlin` — SQL-over-JSON transformation engine
- `kotlin-stdlib` (1.6.20) — required by PartiQL at runtime
- `jackson-databind` — parsing `conf/tasks.json`
- `slf4j-api` + `slf4j-simple` (runtime binding) — logging
