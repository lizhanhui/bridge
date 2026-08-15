# CODEBUDDY.md

This file provides guidance to CodeBuddy Code when working with code in this repository.

## Overview

Early-stage Java (Maven) service that bridges MQTT brokers and RocketMQ: it consumes messages from a source, applies a SQL (PartiQL) transformation to the payload, and publishes to a sink. Source and sink can each be MQTT or RocketMQ, so data can flow in either direction.

## Build & Run

- Build: `mvn compile` (or `mvn package`)
- Entry point: `com.tencent.cloud.mqtt.TaskManager` (currently an empty `main` — the pipeline wiring is in progress)
- No tests or lint tooling are configured yet; `pom.xml` has no test dependencies or plugins
- Compiler target is Java 25 — ensure the JDK matches before building
- Run configuration is defined in `conf/tasks.json` (connectors + tasks)

## Architecture

Data flow: **Source → Transform → Sink**, orchestrated per `Task`.

- `com.tencent.cloud.mqtt` — core abstractions:
  - `Source`, `Sink`, `Transform` — marker interfaces (no methods yet)
  - `Task` — a named Source/Transform/Sink triple with a `launch()` stub
  - `TaskManager` — main entry point; will load `conf/tasks.json`, build connectors and tasks
  - `SQLTransform` — working reference implementation of Transform using PartiQL (`partiql-lang-kotlin`) over JSON payloads. JSON is parsed via ion-java (Ion is a superset of JSON); the message payload is bound to the global name `payload`, so SQL in task configs reads `SELECT ... FROM payload WHERE ...`. Note its `transform()` is currently a hardcoded demo, not yet parameterized.
- `com.tencent.cloud.mqtt.model` — config model POJOs mirroring `conf/tasks.json`:
  - `Connector` — connection credentials; `ConnectorType` enum is `MQTT` / `RocketMQ`
  - `MqttSource` (topic filter, supports MQTT shared subscriptions like `$share/group/...`), `MqttSink` (topic)
  - `RocketMQSource` (consumer group + topic list), `RocketMQSink` (topic)

`conf/tasks.json` schema: a `connectors` array (id/type/access_point/username/password) and a `tasks` array; each task references connectors by `connector_id` and carries a `sql` PartiQL statement executed against the incoming payload.

## Key dependencies

- `hivemq-mqtt-client` — MQTT connectivity
- `rocketmq-client` (5.5.0) — RocketMQ connectivity
- `partiql-lang-kotlin` — SQL-over-JSON transformation engine
- `kotlin-stdlib` (1.6.20) — required by PartiQL at runtime
- `slf4j-api` — logging facade (no binding configured yet)
