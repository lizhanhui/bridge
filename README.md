# MQTT–RocketMQ–Kafka Bridge

A Java service that consumes messages from MQTT, RocketMQ, or Kafka, transforms JSON payloads with PartiQL, and publishes the results to MQTT, RocketMQ, or Kafka.

Supported flows:

- MQTT → RocketMQ
- RocketMQ → MQTT
- MQTT → MQTT
- RocketMQ → RocketMQ
- MQTT → Kafka
- RocketMQ → Kafka
- Kafka → MQTT
- Kafka → RocketMQ
- Kafka → Kafka

> **Production status:** production ready. The publish-before-ack pipeline is unit-tested, sink failures are retried with exponential backoff, and both connector families are covered by automated broker-backed integration tests (require Docker): `MqttBridgeIntegrationTest` boots a throwaway HiveMQ broker via Testcontainers, and `RocketMQBridgeIntegrationTest` boots a throwaway Apache RocketMQ 5.x cluster (NameServer + broker + proxy) via Testcontainers. Both suites exercise transformation and projection, SQL filtering, hop-count loop prevention, malformed-payload handling, and `src.*.topic` provenance / `dst.*.topic` routing against a real broker. Both bridge directions (MQTT → RocketMQ with filter and projection tasks, and RocketMQ → MQTT) have additionally been verified end-to-end against live TDMQ MQTT/RocketMQ clusters, including hop-count loop prevention (see `e2e/E2EClient.java`). Remaining hardening items are tracked in [docs/issues.md](docs/issues.md). The checked-in sample configurations contain only template placeholders — supply real endpoints and credentials at deploy time and never commit them.

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
MQTT, RocketMQ, or Kafka source
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
 MQTT, RocketMQ, or Kafka sink
```

The source and sink are selected independently, so the same processing model supports all nine source/sink combinations. A task can also be expanded into multiple processing lanes for higher throughput.

The bridge intentionally uses a small set of core abstractions:

- `Source` blocks while polling and returns an acknowledgement-capable record.
- `Transform` evaluates a precompiled PartiQL statement against the record payload.
- `Sink` performs a blocking publish and propagates failures.
- `Task` coordinates processing and acknowledges the source only after processing completes.
- `TaskManager` reads configuration, constructs lanes, starts virtual threads, and handles shutdown signals.

Kafka brokers are required only for tasks using a Kafka source or sink. All flows reuse Kafka Streams' `Record` and Kafka's native header types; no Kafka Streams topology is involved.

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

#### Kafka

- Uses the native Kafka producer, one instance per lane.
- Publishes strings and preserves the record key, timestamp, and native headers.
- Waits for broker-confirmed delivery before the task acknowledges the source.
- Defaults to `acks=all` and producer idempotence; delivery remains at-least-once across the bridge.
- Closes with a bounded 10-second wait.
- Sources use a dedicated consumer-owner thread per lane, keeping polling active while sink processing runs.
- Sources buffer one poll batch, process records sequentially, and batch only completed per-partition offsets for commit.
- Source acknowledgement records in-memory completion; commits may lag and replay completed records after a crash/rebalance.
- Transactions spanning source and sink are not supported.

## Prerequisites

- JDK 25
- Maven 3.x
- Access to the MQTT 5 broker, RocketMQ 5.x proxy, or Kafka brokers required by the configured flow
- Existing RocketMQ topics and consumer groups as required by your broker environment
- Kafka bootstrap endpoints and permission to consume source topics/use groups or publish destination topics, as appropriate

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
| `type` | Yes | Exactly `MQTT`, `RocketMQ`, or `Kafka`. |
| `access_point` | MQTT/RocketMQ only | Broker endpoint; format depends on connector type. Not required for Kafka. |
| `username` | No | MQTT/RocketMQ authentication username. |
| `password` | No | MQTT/RocketMQ authentication password. Used only when a non-blank username is set. |
| `properties` | Kafka only | Native Kafka producer settings; `bootstrap.servers` is required inside this object. |

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

#### Kafka connector

```json
{
  "id": "kafka-a",
  "type": "Kafka",
  "properties": {
    "bootstrap.servers": "kafka:9092",
    "compression.type": "lz4"
  }
}
```

Kafka uses **native Kafka property names** inside `properties`. It does not require `access_point`. `bootstrap.servers` is required and accepts a comma-separated string or a non-empty list of server strings. Property values may be strings, numbers, booleans, or lists of strings; nested objects and null values are rejected.

| Property | Default | Notes |
| --- | --- | --- |
| `bootstrap.servers` | Required | Kafka bootstrap endpoints. |
| `client.id` | `sink-<task-name>-<lane>` | One producer per lane; can be overridden. |
| `acks` | `all` | `0` is rejected because publication must wait for broker confirmation. |
| `enable.idempotence` | `true` | Protects producer-internal retries, not bridge resends or source redelivery. |
| `delivery.timeout.ms` | `120000` | Kafka default; bounds producer delivery attempts. |
| `max.block.ms` | `60000` | Kafka default; bounds metadata/buffer waits in `send()`. |
| `compression.type` | `none` | Kafka default; `lz4` is one optional choice. |
| `security.protocol` | `PLAINTEXT` | Kafka default; explicitly configure TLS/SASL for secured clusters. |

Other settings retain Kafka's defaults and may be supplied using their native names. Overrides must remain consistent with Kafka's idempotence constraints; invalid combinations fail at startup. Key and value serializers are bridge-owned `StringSerializer` instances (UTF-8 by default); `key.serializer` and `value.serializer` overrides are rejected. Kafka sink transactions are not supported, so `transactional.id` is rejected when constructing a producer. Source-specific settings and restrictions are described below.

Authentication is configured entirely through native Kafka properties; top-level `username`/`password` are not translated for Kafka. For example, a SASL/PLAIN connection over TLS uses:

```json
{
  "bootstrap.servers": "kafka:9093",
  "security.protocol": "SASL_SSL",
  "sasl.mechanism": "PLAIN",
  "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<kafka-username>\" password=\"<kafka-password>\";"
}
```

Place these settings inside the connector's `properties` object. Use `ssl.truststore.*` or other native TLS settings when your cluster requires them, and protect credential-bearing configuration files.

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

### Kafka source

```json
{
  "id": "kafka-source",
  "type": "Kafka",
  "properties": {
    "bootstrap.servers": "kafka:9092",
    "group.id": "bridge-workers",
    "auto.offset.reset": "earliest"
  }
}
```

Kafka sources require a nonblank native `group.id` and an **explicit** `auto.offset.reset` under connector `properties`. Choose `earliest` for retained backlog, `latest` for future-only consumption, or another policy accepted by Kafka. This applies when no usable committed offset exists; normal restarts resume committed offsets. The bridge does not choose this policy for you.

```json
{
  "connector_id": "kafka-source",
  "topics": ["sensor-events", "device-events"],
  "commit_interval_ms": 1000
}
```

| Source field | Required | Default | Description |
| --- | --- | --- | --- |
| `connector_id` | Yes | — | Kafka connector ID. |
| `topics` | Yes | — | Nonempty array of valid exact topic names; no regex or manual partition assignments. |
| `commit_interval_ms` | No | `1000` | Positive integer interval for batching **completed** offsets; not Kafka's `auto.commit.interval.ms`. |

| Consumer property | Default | Notes |
| --- | --- | --- |
| `group.id` | Required | Source tasks sharing this group compete for records. |
| `auto.offset.reset` | Explicit choice required | No implicit earliest/latest policy. |
| `client.id` | `source-<task-name>-<lane>` | Native override allowed. |
| `enable.auto.commit` | `false` | `true` is rejected; unprocessed records must not be committed. |
| `isolation.level` | `read_committed` | Avoids forwarding aborted transactions; native `read_uncommitted` override allowed. |
| `max.poll.records` | `500` | Native default, configurable; bounds each handoff batch, not the client fetch cache. |
| `group.protocol` | `classic` | Only the classic protocol is supported initially. |

Key/value deserializers are bridge-owned `StringDeserializer` instances (UTF-8 by default); overrides are rejected. Static `group.instance.id` and nonempty consumer `interceptor.classes` are not supported initially: static identities can fence parallel lanes, and mutating interceptors can invalidate offset/order assumptions. Other native consumer settings, including security and fetch tuning, are retained and validated by Kafka.

A connector can be reused as a source and sink with compatible native settings. Each client copies properties and applies its own defaults/restrictions; opposite-role settings may produce unused-key logs. Use separate connectors for role-specific plugins, different explicit `client.id` values, or different consumer groups. Sharing a source connector across SQL tasks does **not** create independent fan-out: each independent pipeline needs a different `group.id`.

Null-valued Kafka tombstones are warned about and skipped using completed-offset accounting. They do not enter SQL or propagate as deletions.

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

### Kafka sink

```json
{
  "connector_id": "kafka-a",
  "topic": "processed-events"
}
```

All sink types require `connector_id` and `topic`. The Kafka sink supports `dst.kafka.topic` routing overrides; there is no task-level partition setting.

For example, with the MQTT, RocketMQ, and Kafka connectors above, either source can target Kafka:

```json
[
  {
    "name": "mqtt-to-kafka",
    "source": {
      "connector_id": "mqtt-a",
      "topic_filter": "$share/kafka-workers/sensors/#"
    },
    "sql": "SELECT payload.deviceId, payload.temperature FROM payload",
    "sink": {"connector_id": "kafka-a", "topic": "mqtt-events"}
  },
  {
    "name": "rocketmq-to-kafka",
    "source": {
      "connector_id": "rocketmq-a",
      "consumer_group": "kafka-workers",
      "topics": ["sensor-events"]
    },
    "sql": "SELECT * FROM payload",
    "sink": {"connector_id": "kafka-a", "topic": "rocketmq-events"}
  }
]
```

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

### Kafka sources

Lanes use the native `group.id` and load-balance assigned partitions. More lanes than partitions leave extra lanes idle. Processing is sequential per lane; ordering across partitions/lanes is not guaranteed. A slow sink blocks that lane's buffered and newly assigned records while the owner keeps polling paused partitions. Increase partitions and parallelism for concurrency.

Each lane creates separate source and sink clients, so increasing parallelism also increases broker connections and resource use.

## Delivery and acknowledgement semantics

The bridge is designed for at-least-once processing on acknowledgement-capable paths:

- The source is not acknowledged before transformation.
- Filtered records are acknowledged.
- Multi-row results are acknowledged only after every output is published.
- A recoverable sink failure is retried indefinitely before acknowledgement; the source record remains unacknowledged while retries continue.
- A sink validation failure (`IllegalArgumentException`) is treated as a poison record: the failing output and remaining outputs from that source record are skipped, a warning is logged, and the source record is acknowledged.
- Source acknowledgements are idempotent within one process.
- Kafka source `ack()` marks processing complete in memory, not a durable broker commit. The owner periodically commits only completed per-partition prefixes; queued/unfinished offsets are never committed. Shutdown/revocation attempts a best-effort final completed-prefix flush.
- Kafka tombstones are skipped and marked complete without SQL evaluation.

This design allows duplicates. For example:

- If two output rows are published and the third publish keeps failing, the unacknowledged source record can be redelivered and all rows generated again.
- If `sink.publish` throws after the broker already accepted the message (for example, a client-side timeout or connection drop), the in-process retry republishes the message, producing a duplicate at the sink without any redelivery.
- On RocketMQ sources, a lane still retrying past the 30-second invisible duration lets another lane in the same consumer group receive and process the same message concurrently; both lanes eventually ack.

Kafka source records published since the last successful batched commit can replay after a crash, failed flush, or rebalance. The commit interval is a scheduling target, not a maximum replay bound during commit failures. A revoked partition can replay elsewhere while old sink work finishes; a stale acknowledgement never commits into the new assignment.

Sink consumers should therefore be idempotent when duplicate delivery matters.

Important boundaries:

- MQTT QoS 0 messages cannot provide at-least-once delivery because the broker has no acknowledgement to redeliver after a crash.
- An MQTT source subscribes at QoS 1, but subscription QoS does not upgrade a publisher's QoS 0 message.
- The incoming `mqtt.qos` header currently overrides the MQTT sink's default QoS 1, so QoS 0 can propagate through the bridge.
- RocketMQ messages use a fixed 30-second invisible duration. Processing longer than 30 seconds can cause concurrent redelivery because invisibility is not renewed.
- The bridge does not provide exactly-once delivery or distributed transactions across brokers.
- Kafka producer idempotence does not deduplicate a new bridge `send()` call, application retries, or replay after a restart. Kafka's delivery/metadata waits can exceed RocketMQ's 30-second invisibility window.

## Loop prevention

Every pass adds or increments the `bridge-hop-count` record header. It is carried between brokers as an MQTT user property, RocketMQ message property, or Kafka header.

`max_hops` defaults to `1`. The current implementation drops a record only when its incoming count is **greater than** `max_hops`, then acknowledges it. Consequently, a record whose count equals the limit is allowed through once more. With `max_hops: 1`, a fresh record can pass through two bridge tasks before the next task drops it.

Malformed hop counts are currently treated as zero, and negative values are not rejected. Do not rely on this mechanism as a security boundary.

## Metadata mapping

The bridge preserves protocol metadata in record headers. PartiQL changes the value but reuses the input key, timestamp, and headers for every result row.

### MQTT to record

| MQTT value | Record representation |
| --- | --- |
| Topic | `src.mqtt.topic` header (informational, propagates downstream) |
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

- `dst.mqtt.topic` overrides the sink's configured topic; it is consumed on publish and never propagated.
- `mqtt.qos` overrides the default QoS 1.
- Other reserved `mqtt.*` headers configure retain, content type, correlation data, response topic, and expiry. `mqtt.duplicate` and `mqtt.message.packet.id` are reserved but have no effect on publication.
- Non-reserved headers become MQTT user properties.
- Headers beginning with `$__` are dropped.

### RocketMQ to record

| RocketMQ value | Record representation |
| --- | --- |
| Topic | `src.rmq.topic` header (informational, propagates downstream) |
| Message ID | Record key and `rmq.message.id` header |
| Tag | `rmq.tag` header |
| Keys | Comma-separated `rmq.keys` header |
| Born timestamp | Record timestamp and `rmq.born.timestamp` header |
| Delivery timestamp | `rmq.delivery.timestamp` header |
| Delivery attempt | `rmq.delivery.attempt` header |
| Born host | `rmq.born.host` header |
| User properties | Same-name record headers |

### Record to RocketMQ

- `dst.rmq.topic` overrides the sink's configured topic; it is consumed on produce and never propagated.
- `rmq.tag` and `rmq.keys` configure the outgoing message.
- Non-reserved headers become RocketMQ properties.
- Headers beginning with `$__` are dropped.

### Kafka to record

| Kafka value | Record representation |
| --- | --- |
| Key/value | Original strings; null keys retained; null-valued tombstones skipped by the source |
| Timestamp | Original nonnegative timestamp; receive time when Kafka reports no/negative timestamp |
| Topic | Authoritative `src.kafka.topic` header |
| Partition/offset | Authoritative decimal `kafka.partition` / `kafka.offset` headers and private ack context |
| Headers | Ordered same-name headers with duplicate/binary/null values preserved and byte arrays copied |

Only upstream copies of the three generated metadata names are replaced. Other `kafka.*`, cross-protocol metadata, `$__*`, hop counts and `dst.*` hints remain unchanged. Partition/offset headers are informational, not destination partition routing. MQTT/RocketMQ sinks keep their existing string-property/protocol constraints.

### Record to Kafka

- Preserve the string key and transformed value, using string serializers (UTF-8 by default).
- Preserve the record timestamp: MQTT receive time, RocketMQ born time, or the incoming Kafka timestamp. Kafka broker timestamp policies still apply.
- Leave partition selection to Kafka. MQTT/RocketMQ keys are message IDs; Kafka source keys are preserved. Ordering depends on source partitions and lane/producer configuration.
- Preserve header order, duplicate names, binary values, and null values, including `src.*.topic`, `bridge-hop-count`, and MQTT/RocketMQ metadata.
- Unlike MQTT/RocketMQ outbound mapping, retain `$__*` source annotations as metadata; Kafka does not regenerate MQTT broker annotations.
- The last `dst.kafka.topic` header overrides the configured topic; a null value falls back to the configured topic. All copies of this routing header are omitted from the outgoing headers without mutating the input record.
- Other protocols' metadata is informational: MQTT expiry and RocketMQ scheduling headers do not enable Kafka TTL or delayed delivery.

> **Security note:** `dst.mqtt.topic` / `dst.rmq.topic` / `dst.kafka.topic` are a deliberate routing-hint channel: any client able to publish to a source topic can reroute that task's sink output. This is intended for trusted upstreams, not a security boundary. If source publishers are untrusted, scope the bridge's producer ACLs to the topics it is allowed to reach.

## Operations and failure behavior

### Sink retries

When `sink.publish` fails, `Task` classifies the failure:

- Most runtime failures are treated as recoverable. The same output is retried indefinitely with exponential backoff: 1, 2, 4, 8, 16, 32, then 60 seconds between attempts. Interrupting the lane stops the retry loop.
- `IllegalArgumentException` is treated as a poison record because retrying an invalid destination or message will not help. The failing output and any remaining outputs from the same source record are skipped with a warning, and the source record is acknowledged.

The Kafka sink classifies record-specific invalid-topic, invalid-timestamp, oversized-record, and serialization failures as poison errors. Connectivity, timeout, authentication, and authorization errors are not poison: they retain the source acknowledgement and follow the existing retry policy. Configuration errors are rejected at startup; interrupted Kafka publication leaves the source unacknowledged.

Retries occur before source acknowledgement, so broker redelivery remains the fallback if the process stops while retrying. A source, transformation, or acknowledgement failure still terminates the affected lane; `TaskManager` logs the error but does not restart the lane or terminate the process. External monitoring must inspect logs and message flow; process liveness alone is insufficient.

### Backpressure

Each MQTT source lane has a bounded queue of 10,000 records. When the queue remains full for five seconds, the callback leaves the new message unacknowledged. Sustained saturation can consume MQTT inflight capacity and stall delivery until reconnection.

RocketMQ receives one message at a time per lane and does not use the MQTT queue.

Kafka sources buffer at most one returned poll batch including its active record, bounded by native `max.poll.records`. The owner pauses assigned partitions while work is outstanding and continues polling for membership/rebalances. Native `fetch.*` settings govern extra fetch-cache memory. Transient commit failures retain completed offsets for a later fresh, lease-validated commit; authentication, authorization, fencing or unexpected consumer errors stop the lane and interrupt its processing thread.

### Shutdown

On shutdown, all lane threads are interrupted. Interrupting a lane also interrupts any in-progress sink retry backoff. Each lane then attempts to close its source followed by its sink. Kafka source close wakes the owner and best-effort flushes completed offsets before bounded cleanup; queued/unfinished records are never committed. The shutdown hook does not wait for cleanup, so allow the process an external termination grace period and do not assume every close operation completes.

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

The test suite covers task processing, native Kafka configuration/defaults, producer/consumer completion, batching, rebalance, error and interruption behavior, PartiQL transformation, and offline protocol mapping. Broker-backed tests include `MqttBridgeIntegrationTest` (HiveMQ), `RocketMQBridgeIntegrationTest` (Apache RocketMQ), and `KafkaBridgeIntegrationTest` (MQTT/RocketMQ↔Kafka) and `KafkaSourceIntegrationTest` (Kafka-source offset/replay and polling behavior). A running Docker daemon is required for these suites. The custom RocketMQ container requires host port `8080` to be available. Testcontainers dependencies use a locally published snapshot; the Kafka module must be published under the same version as the other modules.

## Known limitations

Read [docs/issues.md](docs/issues.md) before production use. Notable limitations include:

- Source-controlled topic headers can override configured sink topics.
- Failed lanes are not restarted and do not fail the process; recoverable sink publication failures are retried, but other lane failures still terminate the lane.
- MQTT queue saturation can stall inflight delivery.
- RocketMQ's 30-second invisibility period is fixed and not renewed.
- MQTT QoS 0 is outside the at-least-once guarantee.
- MQTT TLS is inferred only from port `8883`.
- Configuration validation is permissive outside Kafka-specific validated fields.
- Kafka batched commits can replay completed records; source tombstones are deliberately skipped.
- Kafka sources initially support only classic groups without static membership or consumer interceptors.
- Malformed payloads are acknowledged and logged in full.
- Transform output has no row, byte, or memory limit.
- Shutdown does not wait for resource cleanup.
- The shaded JAR currently emits duplicate class/resource warnings for some dependencies.

Do not describe this version as exactly-once or fully graceful; the delivery semantics are at-least-once with possible duplicates, and shutdown is best-effort.
