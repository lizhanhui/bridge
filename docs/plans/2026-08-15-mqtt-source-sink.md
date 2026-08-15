# MQTT Source & Sink Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement MQTT source/sink connectors and wire the source → transform → sink pipeline loop in `Task`.

**Architecture:** Pull model — the MQTT client's async callback feeds a bounded `BlockingQueue` inside `MqttSource`; `Task.launch()` loops `source.poll()` → `transform()` → `sink.publish()`. Fail-fast on publish errors; interrupt-driven shutdown closes both ends. Design details: `docs/plans/2026-08-15-mqtt-source-sink-design.md`.

**Tech Stack:** Java 25, hivemq-mqtt-client 1.3.17 (MQTT 5), kafka-streams `Record`, JUnit 5, Maven.

---

### Task 1: Source/Sink interfaces + Task pipeline loop

**Files:**
- Modify: `src/main/java/com/tencent/cloud/mqtt/Source.java`
- Modify: `src/main/java/com/tencent/cloud/mqtt/Sink.java`
- Modify: `src/main/java/com/tencent/cloud/mqtt/Task.java`
- Test: `src/test/java/com/tencent/cloud/mqtt/TaskTest.java`

Note: `model/MqttSource.java` etc. still implement the old marker interfaces — they will not compile after this task. That is expected; they are deleted in Task 4. To keep the build green per-commit, do Task 4's deletions *as part of this task's commit* (delete `model/MqttSource.java`, `model/MqttSink.java`, `model/RocketMQSource.java`, `model/RocketMQSink.java`) and temporarily make `TaskManager.parseSource/parseSink` throw `UnsupportedOperationException` for all branches (full re-wiring in Task 4). Alternatively do Tasks 1 and 4 as one commit — but keep tests passing before committing.

**Step 1: Write the failing test**

```java
package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

class TaskTest {

    static class FakeSource implements Source {
        final LinkedBlockingQueue<Record<String, String>> records = new LinkedBlockingQueue<>();
        boolean closed;

        @Override
        public Record<String, String> poll() throws InterruptedException {
            // null when closed and drained, like MqttSource
            while (true) {
                Record<String, String> r = records.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (r != null) return r;
                if (closed && records.isEmpty()) return null;
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    static class FakeSink implements Sink {
        final List<Record<String, String>> published = new ArrayList<>();
        boolean closed;

        @Override
        public void publish(Record<String, String> record) {
            published.add(record);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Test
    void recordsFlowFromSourceThroughTransformToSink() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(new Record<>("home/room/1", "{\"age\":20}", 1L));
        FakeSink sink = new FakeSink();
        // identity transform
        Task task = new Task("t", source, r -> Optional.of(List.of(r)), sink);

        Thread thread = Thread.ofVirtual().start(task::launchUnchecked);
        // wait until the record reaches the sink, then stop the task
        long deadline = System.currentTimeMillis() + 5000;
        while (sink.published.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        thread.interrupt();
        thread.join();

        assertEquals(1, sink.published.size());
        assertEquals("{\"age\":20}", sink.published.get(0).value());
        assertTrue(source.closed);
        assertTrue(sink.closed);
    }

    @Test
    void filteredRecordsAreNotPublished() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(new Record<>("home/room/1", "{\"age\":99}", 1L));
        FakeSink sink = new FakeSink();
        // transform filters everything out
        Task task = new Task("t", source, r -> Optional.empty(), sink);

        Thread thread = Thread.ofVirtual().start(task::launchUnchecked);
        Thread.sleep(200); // give the loop time to consume the record
        thread.interrupt();
        thread.join();

        assertTrue(sink.published.isEmpty());
        assertTrue(source.closed);
        assertTrue(sink.closed);
    }
}
```

Add the `launchUnchecked` helper only inside the test source set is not possible — instead declare the lambda as throwing: use `Thread.ofVirtual().start(() -> { try { task.launch(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } })`. Replace `task::launchUnchecked` accordingly in both tests.

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=TaskTest`
Expected: compilation failure — `Source`/`Sink` have no `poll`/`publish` methods.

**Step 3: Implement interfaces and Task loop**

`Source.java`:

```java
package com.tencent.cloud.mqtt;

import org.apache.kafka.streams.processor.api.Record;

public interface Source extends AutoCloseable {
    // Blocks until a record is available; returns null when closed and drained
    Record<String, String> poll() throws InterruptedException;

    @Override
    void close();
}
```

`Sink.java`:

```java
package com.tencent.cloud.mqtt;

import org.apache.kafka.streams.processor.api.Record;

public interface Sink extends AutoCloseable {
    void publish(Record<String, String> record);

    @Override
    void close();
}
```

`Task.java`:

```java
package com.tencent.cloud.mqtt;

import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
    private static final Logger log = LoggerFactory.getLogger(Task.class);

    private final String name;

    private final Source source;

    private final Transform<String, String> transform;

    private final Sink sink;

    public Task(String name, Source source, Transform<String, String> transform, Sink sink) {
        this.name = name;
        this.source = source;
        this.transform = transform;
        this.sink = sink;
    }

    public String getName() {
        return name;
    }

    public void launch() throws InterruptedException {
        log.info("Task {} started", name);
        try {
            Record<String, String> record;
            while ((record = source.poll()) != null) {
                transform.transform(record)
                    .ifPresent(records -> records.forEach(sink::publish));
            }
        } finally {
            source.close();
            sink.close();
            log.info("Task {} stopped", name);
        }
    }
}
```

Also delete the four model POJOs and stub out TaskManager branches (see header note): in `TaskManager.java`, replace the bodies of `parseSource`/`parseSink` with a temporary `throw new UnsupportedOperationException("not yet implemented")` and remove the now-unused model imports.

**Step 4: Run test to verify it passes**

Run: `mvn test`
Expected: PASS (TaskTest 2 tests, SQLTransformTest still green).

**Step 5: Commit**

```bash
git add -A
git commit -m "Wire source->transform->sink pipeline loop in Task"
```

---

### Task 2: Shared MQTT client builder

**Files:**
- Create: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttClients.java`
- Test: `src/test/java/com/tencent/cloud/mqtt/mqtt/MqttClientsTest.java`

**Step 1: Write the failing test**

```java
package com.tencent.cloud.mqtt.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MqttClientsTest {

    @Test
    void parsesHostAndPort() {
        MqttClients.HostPort hp = MqttClients.parseAccessPoint("mqtt.example.com:1883");
        assertEquals("mqtt.example.com", hp.host());
        assertEquals(1883, hp.port());
        assertFalse(hp.ssl());
    }

    @Test
    void port8883EnablesSsl() {
        MqttClients.HostPort hp = MqttClients.parseAccessPoint("mqtt.example.com:8883");
        assertEquals(8883, hp.port());
        assertTrue(hp.ssl());
    }

    @Test
    void defaultsToPort1883WithoutSsl() {
        MqttClients.HostPort hp = MqttClients.parseAccessPoint("mqtt.example.com");
        assertEquals("mqtt.example.com", hp.host());
        assertEquals(1883, hp.port());
        assertFalse(hp.ssl());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=MqttClientsTest`
Expected: compilation failure — class does not exist.

**Step 3: Implement**

```java
package com.tencent.cloud.mqtt.mqtt;

import java.nio.charset.StandardCharsets;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.tencent.cloud.mqtt.model.Connector;

/** Builds hivemq MQTT5 clients from connector config. */
final class MqttClients {

    record HostPort(String host, int port, boolean ssl) {}

    private MqttClients() {}

    static HostPort parseAccessPoint(String accessPoint) {
        String host = accessPoint;
        int port = 1883;
        int colon = accessPoint.lastIndexOf(':');
        if (colon >= 0) {
            host = accessPoint.substring(0, colon);
            port = Integer.parseInt(accessPoint.substring(colon + 1));
        }
        return new HostPort(host, port, port == 8883);
    }

    static Mqtt5AsyncClient buildAsyncClient(Connector connector, String clientIdSuffix) {
        HostPort hp = parseAccessPoint(connector.getAccessPoint());
        Mqtt5ClientBuilder builder = MqttClient.builder()
            .identifier("bridge-" + connector.getId() + "-" + clientIdSuffix)
            .serverHost(hp.host())
            .serverPort(hp.port())
            .useMqttVersion5();
        if (hp.ssl()) {
            builder = builder.sslWithDefaultConfig();
        }
        builder = builder.simpleAuth()
            .username(connector.getUsername())
            .password(connector.getPassword().getBytes(StandardCharsets.UTF_8))
            .applySimpleAuth();
        return builder.buildAsync();
    }
}
```

Note: `MqttClient.builder()` returns an `Mqtt5ClientBuilder` in hivemq-mqtt-client 1.3.x (`.useMqttVersion5()` is not needed — drop that line; `MqttClient.builder()` is already MQTT 5). Verify against compile errors and adjust: the correct minimal form is `Mqtt5ClientBuilder builder = MqttClient.builder().identifier(...).serverHost(...).serverPort(...);`.

**Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=MqttClientsTest`
Expected: PASS (3 tests).

**Step 5: Commit**

```bash
git add src/main/java/com/tencent/cloud/mqtt/mqtt/MqttClients.java src/test/java/com/tencent/cloud/mqtt/mqtt/MqttClientsTest.java
git commit -m "Add shared hivemq client builder for MQTT connectors"
```

---

### Task 3: MqttSource

**Files:**
- Create: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSource.java`

No unit test (needs a live broker); queue/shutdown semantics are exercised by `TaskTest` fakes and verified manually in Task 5.

**Step 1: Implement**

```java
package com.tencent.cloud.mqtt.mqtt;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.tencent.cloud.mqtt.Source;
import com.tencent.cloud.mqtt.model.Connector;

/**
 * Subscribes to an MQTT topic filter and exposes messages via {@link #poll()}.
 * The hivemq async callback feeds a bounded queue, giving natural backpressure;
 * when the queue stays full the message is dropped (logged) rather than
 * stalling the client.
 */
public class MqttSource implements Source {
    private static final Logger log = LoggerFactory.getLogger(MqttSource.class);

    private static final int QUEUE_CAPACITY = 10_000;
    private static final long OFFER_TIMEOUT_MS = 5_000;

    private final Mqtt5AsyncClient client;
    private final LinkedBlockingQueue<Record<String, String>> queue =
        new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private volatile boolean closed;

    public MqttSource(Connector connector, String topicFilter) {
        this.client = MqttClients.buildAsyncClient(connector, "source");
        client.toBlocking().connect();
        client.toAsync().subscribeWith()
            .topicFilter(topicFilter)
            .qos(MqttQos.AT_LEAST_ONCE)
            .callback(publish -> {
                Record<String, String> record = new Record<>(
                    publish.getTopic().toString(),
                    new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8),
                    System.currentTimeMillis());
                try {
                    if (!queue.offer(record, OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                        log.error("Source queue full for {}s, dropping message from topic {}",
                            OFFER_TIMEOUT_MS / 1000, publish.getTopic());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            })
            .send()
            .join();
        log.info("MqttSource subscribed to {} on {}", topicFilter, connector.getAccessPoint());
    }

    @Override
    public Record<String, String> poll() throws InterruptedException {
        while (true) {
            Record<String, String> record = queue.poll(500, TimeUnit.MILLISECONDS);
            if (record != null) {
                return record;
            }
            if (closed && queue.isEmpty()) {
                return null;
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        client.toBlocking().disconnect();
    }
}
```

**Step 2: Compile**

Run: `mvn compile`
Expected: BUILD SUCCESS.

**Step 3: Commit**

```bash
git add src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSource.java
git commit -m "Add MQTT source backed by hivemq client"
```

---

### Task 4: MqttSink + TaskManager wiring

**Files:**
- Create: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSink.java`
- Modify: `src/main/java/com/tencent/cloud/mqtt/TaskManager.java` (parseSource/parseSink, imports)

**Step 1: Implement MqttSink**

```java
package com.tencent.cloud.mqtt.mqtt;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.tencent.cloud.mqtt.Sink;
import com.tencent.cloud.mqtt.model.Connector;

/** Publishes records to a fixed MQTT topic. Fail-fast: publish errors propagate and kill the task. */
public class MqttSink implements Sink {
    private static final Logger log = LoggerFactory.getLogger(MqttSink.class);

    private final Mqtt5AsyncClient client;
    private final String topic;

    public MqttSink(Connector connector, String topic) {
        this.client = MqttClients.buildAsyncClient(connector, "sink");
        this.topic = topic;
        client.toBlocking().connect();
        log.info("MqttSink connected to {} for topic {}", connector.getAccessPoint(), topic);
    }

    @Override
    public void publish(Record<String, String> record) {
        client.toBlocking().publishWith()
            .topic(topic)
            .qos(MqttQos.AT_LEAST_ONCE)
            .payload(record.value().getBytes(StandardCharsets.UTF_8))
            .send();
    }

    @Override
    public void close() {
        client.toBlocking().disconnect();
    }
}
```

**Step 2: Wire TaskManager**

In `TaskManager.java`:

- Remove imports of `model.MqttSource`, `model.MqttSink`, `model.RocketMQSource`, `model.RocketMQSink`; add imports of `com.tencent.cloud.mqtt.mqtt.MqttSource`, `com.tencent.cloud.mqtt.mqtt.MqttSink`.
- `parseSource`:

```java
private static Source parseSource(JsonNode node, Map<String, Connector> connectors) {
    Connector connector = resolveConnector(node, connectors);
    return switch (connector.getType()) {
        case MQTT -> new MqttSource(connector, node.required("topic_filter").asText());
        case RocketMQ -> throw new UnsupportedOperationException("RocketMQ source not yet implemented");
    };
}
```

- `parseSink`:

```java
private static Sink parseSink(JsonNode node, Map<String, Connector> connectors) {
    Connector connector = resolveConnector(node, connectors);
    return switch (connector.getType()) {
        case MQTT -> new MqttSink(connector, node.required("topic").asText());
        case RocketMQ -> throw new UnsupportedOperationException("RocketMQ sink not yet implemented");
    };
}
```

Note: `conf/tasks.json` contains an rmq2mqtt task — running `TaskManager` against it will now fail fast on the RocketMQ source. That is intended until RocketMQ is implemented; for manual testing use a config with only MQTT tasks (see Task 5).

**Step 3: Verify build and tests**

Run: `mvn test`
Expected: BUILD SUCCESS, all tests pass.

**Step 4: Commit**

```bash
git add -A
git commit -m "Add MQTT sink and wire MQTT connectors into TaskManager"
```

---

### Task 5: Package + manual verification

**Step 1: Package the fat jar**

Run: `mvn package`
Expected: BUILD SUCCESS, `target/bridge-1.0-SNAPSHOT.jar` exists.

**Step 2: Manual smoke test (requires an MQTT broker)**

Create a test config `conf/tasks-mqtt-only.json` with a single MQTT→MQTT task (e.g. subscribe `$share/group-1/home/#`, publish to `home/room/1`, with the sample SQL). Then:

```bash
java -jar target/bridge-1.0-SNAPSHOT.jar conf/tasks-mqtt-only.json
```

In separate terminals, publish a matching JSON message and a non-matching one:

```bash
mosquitto_pub -h <host> -p 8883 -u root -P <password> -t home/room/1 \
  -m '{"age":20,"address":{"number":555}}'   # should be forwarded
mosquitto_pub -h <host> -p 8883 -u root -P <password> -t home/room/1 \
  -m '{"age":50,"address":{"number":555}}'   # should be filtered out
mosquitto_sub -h <host> -p 8883 -u root -P <password> -t home/room/1
```

Expected: only the matching message arrives on the sink topic; Ctrl+C shuts down cleanly ("Task ... stopped" in logs).

**Step 3: Commit (config sample only, if kept)**

```bash
git add conf/tasks-mqtt-only.json
git commit -m "Add MQTT-only sample config for manual verification"
```

(Or delete the file after verification and skip the commit.)
