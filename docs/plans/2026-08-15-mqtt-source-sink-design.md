# MQTT Source & Sink — Design

Date: 2026-08-15
Status: Approved

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
