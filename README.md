# MQTT–RocketMQ Bridge

A Java service that consumes messages from MQTT or RocketMQ, transforms JSON payloads with PartiQL, and publishes the results to MQTT or RocketMQ.

Supported flows:

- MQTT → RocketMQ
- RocketMQ → MQTT
- MQTT → MQTT
- RocketMQ → RocketMQ

> **Production status:** early-stage. The publish-before-ack pipeline is unit-tested, sink failures are retried with exponential backoff, and both bridge directions (MQTT → RocketMQ with filter and projection tasks, and RocketMQ → MQTT) have been verified end-to-end against live TDMQ MQTT/RocketMQ clusters, including hop-count loop prevention (see `e2e/E2EClient.java`). The MQTT source and sink are additionally covered by automated broker-backed integration tests: `MqttBridgeIntegrationTest` boots a throwaway HiveMQ broker via Testcontainers (requires Docker) and exercises transformation and projection, SQL filtering, hop-count loop prevention, malformed-payload handling, and `src.mqtt.topic` provenance / `dst.mqtt.topic` routing. The RocketMQ connectors are not yet covered by broker-backed tests. The service is not yet recommended for production rollout; review [docs/issues.md](docs/issues.md) before deploying it with real traffic. The checked-in sample configurations contain only template placeholders — supply real endpoints and credentials at deploy time and never commit them.

## Table of contents

- [Design](#design)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Writing transformations](#writing-transformations)
- [Parallelism](#parallelism)
- [Delivery and acknowledgement semantics](#delivery-and-acknowledgement-semantics)
- [Loop prevention](#loop-prevention)
- [Metadata mapping](#metadata-mapping)
- [Operations and failure behavior](#operations-and-failure-behavior)
- [Development](#development)
- [Known limitations](#known-limitations)

## Design

Each configured task is an independent **Source → Transform → Sink** pipeline:

```text
MQTT or RocketMQ source
          │
          ▼
  Record<String, String>
          │
          ▼
 PartiQL transformation
   (payload is JSON)
          │
          ▼
 zero, one, or many records
          │
          ▼
 MQTT or RocketMQ sink
```

The source and sink are selected independently, so the same processing model supports all four protocol combinations. A task can also be expanded into multiple processing lanes for higher throughput.

The bridge intentionally uses a small set of core abstractions:

- `Source` blocks while polling and returns an acknowledgement-capable record.
- `Transform` evaluates a precompiled PartiQL statement against the record payload.
- `Sink` performs a blocking publish and propagates failures.
- `Task` coordinates processing and acknowledges the source only after processing completes.
- `TaskManager` reads configuration, constructs lanes, starts virtual threads, and handles shutdown signals.

Kafka brokers are **not** required. The Kafka client dependencies are used only for the `Record` and header types.

## Architecture

### Task lanes

A configured task with `parallelism: N` is expanded into `N` lanes named `<task-name>-0` through `<task-name>-N-1`. Every lane owns its own source, transform, sink, and broker clients, and runs on its own Java virtual thread.

For each source record, a lane:

1. Reads and checks the `bridge-hop-count` header.
2. Drops and acknowledges records over the configured hop limit.
3. Increments the hop count.
4. Evaluates the task's PartiQL statement.
5. Publishes every result row to the sink.
6. Acknowledges the source record after all result rows are published.

A query returning no rows filters the source record out; filtered records are acknowledged.

### Connector implementations

#### MQTT

- Uses the HiveMQ MQTT 5 client.
- Subscribes at QoS 1 with manual acknowledgement.
- Uses a persistent session with `cleanStart=false` and a 24-hour session expiry.
- Enables automatic reconnect with delays from 1 to 120 seconds.
- Buffers up to 10,000 received messages per lane.
- Publishes synchronously from the task's perspective.

#### RocketMQ

- Uses the RocketMQ 5.x Java client and proxy protocol.
- Uses `SimpleConsumer`, receiving one message per call.
- Uses a fixed 30-second invisible duration.
- Uses a blocking producer send.
- Load-balances lanes that use the same consumer group.

## Prerequisites

- JDK 25
- Maven 3.x
- Access to at least one MQTT 5 broker or RocketMQ 5.x proxy, depending on the configured flow
- Existing RocketMQ topics and consumer groups as required by your broker environment

Verify the tools:

```sh
java -version
mvn -version
```

## Quick start

### 1. Build the runnable JAR

```sh
mvn clean package
```

The Maven Shade plugin creates:

```text
target/bridge-1.0-SNAPSHOT.jar
```

The JAR's main class is `com.tencent.cloud.mqtt.TaskManager`.

### 2. Create a configuration

Start from `conf/tasks.json` and replace every `<...>` placeholder with real endpoints and credentials. Never commit the filled-in configuration.

A minimal MQTT-to-RocketMQ configuration is:

```json
{
  "connectors": [
    {
      "id": "mqtt-source",
      "type": "MQTT",
      "access_point": "mqtt.example.com:8883",
      "username": "mqtt-user",
      "password": "replace-me"
    },
    {
      "id": "rocketmq-sink",
      "type": "RocketMQ",
      "access_point": "rocketmq-proxy.example.com:8081",
      "username": "rocketmq-user",
      "password": "replace-me"
    }
  ],
  "tasks": [
    {
      "name": "mqtt-to-rocketmq",
      "source": {
        "connector_id": "mqtt-source",
        "topic_filter": "sensors/#"
      },
      "sql": "SELECT payload.deviceId, payload.temperature FROM payload",
      "sink": {
        "connector_id": "rocketmq-sink",
        "topic": "sensor-events"
      }
    }
  ]
}
```

### 3. Run the bridge

Use the default configuration path:

```sh
java -jar target/bridge-1.0-SNAPSHOT.jar
```

Or pass a custom path:

```sh
java -jar target/bridge-1.0-SNAPSHOT.jar /path/to/tasks.json
```

The default path is `conf/tasks.json`, relative to the process working directory.

### 4. Stop the bridge

Send `SIGTERM` or press Ctrl+C. The shutdown hook interrupts all task lanes, and each lane attempts to close its source and sink. Shutdown is best-effort: the hook does not wait for cleanup to finish before JVM termination.

## Configuration

The root object contains `connectors` and `tasks` arrays:

```json
{
  "connectors": [],
  "tasks": []
}
```

### Connector fields

| Field | Required | Description |
| --- | --- | --- |
| `id` | Yes | Identifier referenced by task source and sink configurations. Keep IDs unique; duplicates are not rejected and the last definition silently wins. |
| `type` | Yes | Exactly `MQTT` or `RocketMQ`. |
| `access_point` | Yes | Broker endpoint; format depends on connector type. |
| `username` | No | Authentication username. |
| `password` | No | Authentication password. Used only when a non-blank username is set. |

#### MQTT connector

```json
{
  "id": "mqtt-a",
  "type": "MQTT",
  "access_point": "mqtt.example.com:8883",
  "username": "user",
  "password": "secret"
}
```

`access_point` uses `host[:port]` format. The default port is `1883`. The current implementation enables TLS only when the port is exactly `8883`; it does not support schemes such as `mqtt://` or `mqtts://`, and it does not support bare IPv6 addresses.

#### RocketMQ connector

```json
{
  "id": "rocketmq-a",
  "type": "RocketMQ",
  "access_point": "rocketmq-proxy.example.com:8081",
  "username": "access-key",
  "password": "secret-key"
}
```

`access_point` must be a RocketMQ 5.x proxy endpoint accepted by the RocketMQ Java client.

### Common task fields

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `name` | Yes | — | Base task name. Lane names are generated by appending `-<sequence>`. |
| `source` | Yes | — | Source configuration. Its connector determines the source protocol. |
| `sql` | Yes | — | PartiQL statement evaluated against the JSON value bound as `payload`. |
| `sink` | Yes | — | Sink configuration. Its connector determines the sink protocol. |
| `parallelism` | No | `1` | Number of independent lanes; must be at least 1. |
| `max_hops` | No | `1` | Loop-prevention threshold. See [Loop prevention](#loop-prevention). |

Task names must produce unique lane names. For example, two tasks named `events` both generate `events-0` and are rejected.

### MQTT source

```json
{
  "connector_id": "mqtt-a",
  "topic_filter": "$share/bridge-workers/sensors/#"
}
```

| Field | Required | Description |
| --- | --- | --- |
| `connector_id` | Yes | ID of an `MQTT` connector. |
| `topic_filter` | Yes | MQTT topic filter, including shared-subscription filters when needed. |

### RocketMQ source

```json
{
  "connector_id": "rocketmq-a",
  "consumer_group": "bridge-consumers",
  "topics": ["sensor-events", "device-events"]
}
```

| Field | Required | Description |
| --- | --- | --- |
| `connector_id` | Yes | ID of a `RocketMQ` connector. |
| `consumer_group` | Yes | Consumer group used by every lane of this task. |
| `topics` | Yes | Non-empty array of subscribed topics. |

### MQTT sink

```json
{
  "connector_id": "mqtt-a",
  "topic": "processed/events"
}
```

### RocketMQ sink

```json
{
  "connector_id": "rocketmq-a",
  "topic": "processed-events"
}
```

Both sink types require `connector_id` and `topic`.

### Complete RocketMQ-to-MQTT example

```json
{
  "connectors": [
    {
      "id": "rocketmq-source",
      "type": "RocketMQ",
      "access_point": "rocketmq-proxy.example.com:8081"
    },
    {
      "id": "mqtt-sink",
      "type": "MQTT",
      "access_point": "mqtt.example.com:8883"
    }
  ],
  "tasks": [
    {
      "name": "rocketmq-to-mqtt",
      "parallelism": 2,
      "source": {
        "connector_id": "rocketmq-source",
        "consumer_group": "bridge-workers",
        "topics": ["device-events"]
      },
      "sql": "SELECT * FROM payload WHERE payload.enabled = true",
      "max_hops": 1,
      "sink": {
        "connector_id": "mqtt-sink",
        "topic": "devices/enabled"
      }
    }
  ]
}
```

## Writing transformations

The record value must be JSON text. It is parsed and bound to the PartiQL global name `payload`. The query is compiled once when the task lane is created and reused for every record.

Given this payload:

```json
{
  "deviceId": "sensor-17",
  "temperature": 23.5,
  "enabled": true,
  "location": {
    "building": "A",
    "floor": 3
  },
  "tags": ["indoor", "critical"]
}
```

### Pass through the complete payload

```sql
SELECT * FROM payload
```

### Select fields

```sql
SELECT payload.deviceId, payload.temperature FROM payload
```

Output:

```json
{"deviceId":"sensor-17","temperature":23.5}
```

### Filter records

```sql
SELECT * FROM payload WHERE payload.enabled = true
```

A query returning no rows filters the source record out and acknowledges it without publishing.

### Select nested fields

```sql
SELECT payload.deviceId, payload.location.building FROM payload
```

### Produce multiple messages

```sql
SELECT VALUE tag FROM payload.tags AS tag
```

This produces one output message per array element. A single input can therefore generate zero, one, or many output messages.

### Reserved field names

PartiQL reserved words must be double-quoted when used as field names:

```sql
SELECT * FROM payload.readings AS r WHERE r."value" > 10
```

Malformed JSON is currently logged and treated as a filtered record, so it is acknowledged and not retried. See [docs/README.md](docs/README.md) for additional SQL notes and [docs/issues.md](docs/issues.md) for the associated production concern.

## Parallelism

Set `parallelism` on a task to create multiple independent lanes:

```json
{
  "name": "sensor-processing",
  "parallelism": 4,
  "source": {},
  "sql": "SELECT * FROM payload",
  "sink": {}
}
```

### MQTT sources

MQTT lanes split traffic only when the source uses a shared subscription:

```text
$share/<group>/<topic-filter>
```

Example:

```json
{
  "topic_filter": "$share/bridge-workers/sensors/#"
}
```

Without `$share/`, every lane creates a separate subscription and receives every matching message, multiplying output. The bridge logs a warning but still starts.

Different shared-subscription groups each receive their own copy of matching traffic. Use the same group to load-balance lanes of one logical pipeline; use different groups when separate tasks must each receive every message.

### RocketMQ sources

RocketMQ lanes use the task's `consumer_group`. Lanes with the same group load-balance messages natively.

Each lane creates separate source and sink clients, so increasing parallelism also increases broker connections and resource use.

## Delivery and acknowledgement semantics

The bridge is designed for at-least-once processing on acknowledgement-capable paths:

- The source is not acknowledged before transformation.
- Filtered records are acknowledged.
- Multi-row results are acknowledged only after every output is published.
- A recoverable sink failure is retried indefinitely before acknowledgement; the source record remains unacknowledged while retries continue.
- A sink validation failure (`IllegalArgumentException`) is treated as a poison record: the failing output and remaining outputs from that source record are skipped, a warning is logged, and the source record is acknowledged.
- Source acknowledgements are idempotent within one process.

This design allows duplicates. For example:

- If two output rows are published and the third publish keeps failing, the unacknowledged source record can be redelivered and all rows generated again.
- If `sink.publish` throws after the broker already accepted the message (for example, a client-side timeout or connection drop), the in-process retry republishes the message, producing a duplicate at the sink without any redelivery.
- On RocketMQ sources, a lane still retrying past the 30-second invisible duration lets another lane in the same consumer group receive and process the same message concurrently; both lanes eventually ack.

Sink consumers should therefore be idempotent when duplicate delivery matters.

Important boundaries:

- MQTT QoS 0 messages cannot provide at-least-once delivery because the broker has no acknowledgement to redeliver after a crash.
- An MQTT source subscribes at QoS 1, but subscription QoS does not upgrade a publisher's QoS 0 message.
- The incoming `mqtt.qos` header currently overrides the MQTT sink's default QoS 1, so QoS 0 can propagate through the bridge.
- RocketMQ messages use a fixed 30-second invisible duration. Processing longer than 30 seconds can cause concurrent redelivery because invisibility is not renewed.
- The bridge does not provide exactly-once delivery or distributed transactions across brokers.

## Loop prevention

Every pass adds or increments the `bridge-hop-count` record header. It is carried between brokers as an MQTT user property or RocketMQ message property.

`max_hops` defaults to `1`. The current implementation drops a record only when its incoming count is **greater than** `max_hops`, then acknowledges it. Consequently, a record whose count equals the limit is allowed through once more. With `max_hops: 1`, a fresh record can pass through two bridge tasks before the next task drops it.

Malformed hop counts are currently treated as zero, and negative values are not rejected. Do not rely on this mechanism as a security boundary.

## Metadata mapping

The bridge preserves protocol metadata in record headers. PartiQL changes the value but reuses the input key, timestamp, and headers for every result row.

### MQTT to record

| MQTT value | Record representation |
| --- | --- |
| Topic | `mqtt.topic` header |
| QoS | `mqtt.qos` header (`0`, `1`, or `2`) |
| Retain flag | `mqtt.retained` header |
| Duplicate flag | `mqtt.duplicate` header; currently always `false` because the client API does not expose it |
| Content type | `mqtt.content.type` header |
| Correlation data | `mqtt.correlation.data` binary header |
| Response topic | `mqtt.response.topic` header |
| Message expiry | `mqtt.message.expiry.interval` header |
| User properties | Same-name record headers |
| `$__messageId` property | Record key |
| Receive time | Record timestamp |

### Record to MQTT

- `mqtt.topic` overrides the sink's configured topic.
- `mqtt.qos` overrides the default QoS 1.
- Other reserved `mqtt.*` headers configure retain, content type, correlation data, response topic, and expiry. `mqtt.duplicate` and `mqtt.message.packet.id` are reserved but have no effect on publication.
- Non-reserved headers become MQTT user properties.
- Headers beginning with `$__` are dropped.

### RocketMQ to record

| RocketMQ value | Record representation |
| --- | --- |
| Topic | `rmq.topic` header |
| Message ID | Record key and `rmq.message.id` header |
| Tag | `rmq.tag` header |
| Keys | Comma-separated `rmq.keys` header |
| Born timestamp | Record timestamp and `rmq.born.timestamp` header |
| Delivery timestamp | `rmq.delivery.timestamp` header |
| Delivery attempt | `rmq.delivery.attempt` header |
| Born host | `rmq.born.host` header |
| User properties | Same-name record headers |

### Record to RocketMQ

- `rmq.topic` overrides the sink's configured topic.
- `rmq.tag` and `rmq.keys` configure the outgoing message.
- Non-reserved headers become RocketMQ properties.
- Headers beginning with `$__` are dropped.

> **Security warning:** because source properties can become record headers, an incoming `mqtt.topic` or `rmq.topic` header can influence the destination used by a sink. Do not grant the bridge producer access to topics that untrusted source publishers must not reach. This behavior is tracked in [docs/issues.md](docs/issues.md).

## Operations and failure behavior

### Sink retries

When `sink.publish` fails, `Task` classifies the failure:

- Most runtime failures are treated as recoverable. The same output is retried indefinitely with exponential backoff: 1, 2, 4, 8, 16, 32, then 60 seconds between attempts. Interrupting the lane stops the retry loop.
- `IllegalArgumentException` is treated as a poison record because retrying an invalid destination or message will not help. The failing output and any remaining outputs from the same source record are skipped with a warning, and the source record is acknowledged.

Retries occur before source acknowledgement, so broker redelivery remains the fallback if the process stops while retrying. A source, transformation, or acknowledgement failure still terminates the affected lane; `TaskManager` logs the error but does not restart the lane or terminate the process. External monitoring must inspect logs and message flow; process liveness alone is insufficient.

### Backpressure

Each MQTT source lane has a bounded queue of 10,000 records. When the queue remains full for five seconds, the callback leaves the new message unacknowledged. Sustained saturation can consume MQTT inflight capacity and stall delivery until reconnection.

RocketMQ receives one message at a time per lane and does not use the MQTT queue.

### Shutdown

On shutdown, all lane threads are interrupted. Interrupting a lane also interrupts any in-progress sink retry backoff. Each lane then attempts to close its source followed by its sink. The shutdown hook does not wait for cleanup, so allow the process an external termination grace period and do not assume every close operation completes.

### Logging

Logging uses SLF4J Simple. Configure it with standard `slf4j-simple` system properties, for example:

```sh
java -Dorg.slf4j.simpleLogger.defaultLogLevel=info \
  -jar target/bridge-1.0-SNAPSHOT.jar /path/to/tasks.json
```

Malformed JSON payloads are currently logged in full. Avoid sending secrets in payloads until this behavior is fixed.

### Credentials

The configuration format currently stores credentials directly in JSON and does not perform environment-variable interpolation. Protect configuration files with filesystem permissions and keep real credentials out of version control. Prefer generating or mounting the runtime configuration from a secret-management system.

## Development

Compile without packaging:

```sh
mvn compile
```

Run all tests:

```sh
mvn test
```

Run one test class:

```sh
mvn test -Dtest=SQLTransformTest
```

Build the runnable fat JAR:

```sh
mvn clean package
```

The current test suite covers task processing, configuration expansion, PartiQL transformation, and offline protocol mapping, plus broker-backed integration tests for the MQTT source and sink (`MqttBridgeIntegrationTest`, Testcontainers HiveMQ — a running Docker daemon is required). The RocketMQ connectors are not yet covered by broker-backed tests.

## Known limitations

Read [docs/issues.md](docs/issues.md) before production use. Current rollout blockers and notable limitations include:

- Source-controlled topic headers can override configured sink topics.
- Failed lanes are not restarted and do not fail the process; recoverable sink publication failures are retried, but other lane failures still terminate the lane.
- MQTT queue saturation can stall inflight delivery.
- RocketMQ's 30-second invisibility period is fixed and not renewed.
- MQTT QoS 0 is outside the at-least-once guarantee.
- MQTT TLS is inferred only from port `8883`.
- Configuration validation is permissive.
- Malformed payloads are acknowledged and logged in full.
- Transform output has no row, byte, or memory limit.
- Shutdown does not wait for resource cleanup.
- The shaded JAR currently emits duplicate class/resource warnings for some dependencies.

Do not describe this version as exactly-once, fully graceful, or production-ready until the corresponding issues are resolved.
