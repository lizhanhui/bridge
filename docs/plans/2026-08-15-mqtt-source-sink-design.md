# MQTT Source & Sink — Design

Date: 2026-08-15
Status: Approved (amended 2026-08-15: record/header mapping, see bottom section)

## Goal

Implement the MQTT side of the bridge: an MQTT `Source` that subscribes to a
topic filter and an MQTT `Sink` that publishes transformed records, wired into
the `Task` pipeline loop. RocketMQ is out of scope.

## Interfaces

Replace the marker interfaces with real contracts:

```java
public interface Source extends AutoCloseable {
    // Blocks until a record is available; returns null when closed and drained.
    Record<String, String> poll() throws InterruptedException;
    @Override void close();
}

public interface Sink extends AutoCloseable {
    void publish(Record<String, String> record);
    @Override void close();
}
```

Record mapping (decided): key = MQTT topic, value = payload as UTF-8 String
(JSON for `SQLTransform`). Sink ignores the key and publishes to its configured
topic.

## Components

New package `com.tencent.cloud.mqtt.mqtt`:

- **`MqttSource`** — hivemq `Mqtt5AsyncClient`. Parses `host:port` from the
  connector `accessPoint`; TLS when port is 8883, plain TCP otherwise; simple
  username/password auth. Connects blocking at startup, subscribes to the
  topic filter at QoS 1 (shared subscriptions `$share/group/...` are native
  MQTT 5). The publish callback maps each message to
  `Record(topic, payload, timestamp)` and offers it to a bounded
  `LinkedBlockingQueue` (capacity 10,000). If the queue stays full, the
  callback blocks briefly and then drops the message with an error log — a
  slow sink must not kill the whole bridge.

- **`MqttSink`** — same connection setup; connects at construction.
  `publish()` sends the value bytes to the configured topic at QoS 1 and joins
  the future. On failure: log and rethrow, which terminates the task (fail-fast;
  an external supervisor restarts the process). `TaskManager` already logs
  task termination.

Both `close()` methods disconnect the client; `MqttSource.close()` also causes
`poll()` to return null once the queue is drained.

## Task wiring

`Task.launch()` becomes the pipeline loop:

```
while (record = source.poll()) != null:
    for out in transform.transform(record) (empty = filtered out):
        sink.publish(out)
```

`InterruptedException` breaks the loop; source and sink are closed in a
`finally`. TaskManager's shutdown hook already interrupts task threads.

## Config model cleanup

Delete the four unused config POJOs `model.MqttSource`, `model.MqttSink`,
`model.RocketMQSource`, `model.RocketMQSink`. `TaskManager.parseSource` /
`parseSink` read JSON fields directly and construct runtime objects.
`Connector` and `ConnectorType` stay. RocketMQ model classes will be re-added
when RocketMQ is implemented.

`conf/tasks.json` schema is unchanged.

## Error handling

- Malformed JSON payloads: dropped by `SQLTransform` (existing behavior).
- Source queue full: block briefly, then drop + error log.
- Publish failure: log, rethrow, task dies (fail-fast).
- Shutdown: interrupt → loop exits → both ends closed.

## Testing

Unit-test the `Task` pipeline loop with fake in-memory Source/Sink:

- records flow source → transform → sink
- filtered-out records (empty transform result) produce no sink publishes
- interrupting the task thread closes source and sink

No broker integration tests for now; manual verification against a real
broker. hivemq-testcontainer can be added later if wanted.

## Amendment (2026-08-15): MQTT ↔ Record header mapping

Supersedes "key = MQTT topic, sink ignores key" above. Mapping follows the
reference diagram (MQTT message ↔ Kafka record), with two corrections:
topic maps to an `mqtt.topic` header (not the key), and the record key is the
`$__messageId` user property. Property names verified against the broker
source (/data/repo/rocketmq-mqtt): the `$__*` names are set by the broker as
MQTT 5 user properties on outbound delivery
(`mqtt-common/.../Constants.java:46-50`, `mqtt-cs/.../Session.java:937-963`);
the `mqtt.*` names are bridge-side record-header conventions (not present in
the broker).

A shared `mqtt.MqttRecordMapper` holds both directions.

### Source — `toRecord(Mqtt5Publish) → Record<String,String>`

| MQTT field | Record |
|---|---|
| payload | value (UTF-8) |
| topic | `mqtt.topic` header |
| QoS | `mqtt.qos` header (`"0"/"1"/"2"`) |
| retain flag | `mqtt.retained` header (`"true"/"false"`) |
| dup flag | `mqtt.duplicate` header (see note) |
| content type | `mqtt.content.type` (when present) |
| correlation data | `mqtt.correlation.data` (raw bytes, when present) |
| response topic | `mqtt.response.topic` (when present) |
| message expiry | `mqtt.message.expiry.interval` (seconds, when present) |
| every user property | header with the same name |
| `$__messageId` user property | record **key** (null if absent) |

`mqtt.message.packet.id` stays a defined constant but is never populated on
subscribe (hivemq does not expose packet IDs for incoming publishes) and
ignored on publish. Likewise `mqtt.duplicate`: hivemq 1.3.17 exposes no dup
flag on delivered publishes (it exists only on the internal
`MqttStatefulPublish`), so the header is always `"false"` until the client
library surfaces it. Record timestamp stays receive-time. `SQLTransform`
already preserves headers on output records, so headers flow through the
transform untouched. User properties whose names collide with reserved
`mqtt.*` headers are skipped (logged) on the source side so they cannot
shadow real protocol values.

### Sink — record headers override global config

Reserved headers are consumed as publish settings, not re-published as user
properties: `mqtt.topic` (overrides configured topic), `mqtt.qos` (overrides
QoS 1), `mqtt.retained`, `mqtt.content.type`, `mqtt.correlation.data`,
`mqtt.response.topic`, `mqtt.message.expiry.interval`.
`mqtt.duplicate` and `mqtt.message.packet.id` are ignored (cannot be set
client-side). `$__`-prefixed headers are **dropped** on publish — they are
broker-assigned metadata and the broker sets fresh ones on delivery;
re-publishing them would produce duplicates. Every other header becomes an
MQTT user property (round-trip). Malformed header values (e.g. non-numeric
`mqtt.qos`) log a warning and fall back to the configured default rather
than failing the task.

### Testing

`MqttRecordMapperTest` (no broker needed — `Mqtt5Publish.builder()`
constructs publishes directly): source direction (key from `$__messageId`,
null when absent, all protocol/user-property headers), sink direction
(topic/QoS overrides, defaults when headers absent, user-property
round-trip, reserved + `$__*` exclusion, malformed-header fallback).
