# HiveMQ Testcontainers Integration Tests Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add automated MQTT-to-MQTT integration tests that run the real `Task` pipeline (MqttSource → SQLTransform → MqttSink) against a throwaway HiveMQ broker via Testcontainers, under plain `mvn test`.

**Architecture:** One test class `MqttBridgeIntegrationTest` with a `@Container static HiveMQContainer` (`hivemq/hivemq-ce:2026.5`). Each test builds a real `Task` on unique `it/<test>/in` → `it/<test>/out` topics, launches it on a virtual thread, and verifies with hivemq-client blocking clients. Design: `docs/plans/2026-08-16-hivemq-testcontainers-it-design.md`.

**Tech Stack:** JUnit 5, `org.testcontainers:{testcontainers,junit-jupiter,hivemq}:1.21.4`, existing `hivemq-mqtt-client` for verification clients.

**Context for the implementer:**
- Production code is already written — these are integration characterization tests, so the TDD "watch it fail" step does not apply. Write each test, run it, expect PASS. An unexpected failure is a real bug finding; stop and report it rather than weakening the assertion.
- `Task` constructor: `new Task(String name, Source source, Transform<String,String> transform, Sink sink, int maxHops)`; `launch()` throws `InterruptedException` and closes source+sink in `finally` when interrupted.
- `MqttSource(Connector, topicFilter, clientIdSuffix)` subscribes in its constructor (blocking `.join()`), so once `startTask` returns the subscription is live — no publish-before-subscribe race.
- `MqttSink(Connector, topic, clientIdSuffix)`.
- `SQLTransform<>(String sql)`; SQL binds the JSON payload as `payload`.
- `Connector` is a POJO with setters; `accessPoint` is `host:port`, no scheme.
- `MqttRecordMapper.toPublish` drops `$__`-prefixed headers, so `$__messageId` (the record key) does NOT round-trip to the outgoing publish — assert on ordinary user properties instead.
- Record headers round-trip as same-name MQTT user properties (except `mqtt.*` reserved and `$__*` broker headers), so `bridge-hop-count` and custom user properties are observable on the sink output.
- JDK 25, virtual threads available.

---

### Task 1: Add Testcontainers dependencies

**Files:**
- Modify: `pom.xml`

**Step 1: Add version property and three test dependencies**

In `<properties>` add:

```xml
<testcontainers.version>1.21.4</testcontainers.version>
```

After the `junit-jupiter` dependency block add:

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>${testcontainers.version}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>${testcontainers.version}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>hivemq</artifactId>
    <version>${testcontainers.version}</version>
    <scope>test</scope>
</dependency>
```

**Step 2: Verify resolution**

Run: `mvn -q dependency:resolve -DincludeScope=test`
Expected: BUILD SUCCESS, `org.testcontainers:hivemq:jar:1.21.4` in the list.

**Step 3: Commit**

```bash
git add pom.xml
git commit -m "Add testcontainers hivemq test dependencies"
```

---

### Task 2: Container smoke test

Validates Docker + image pull + JUnit extension before building real tests on top.

**Files:**
- Create: `src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java`

**Step 1: Write the smoke test**

```java
package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publishes;

@Testcontainers
class MqttBridgeIntegrationTest {

    @Container
    static final HiveMQContainer HIVEMQ =
        new HiveMQContainer(DockerImageName.parse("hivemq/hivemq-ce:2026.5"));

    @Test
    void brokerRoundTrip() throws Exception {
        Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier("smoke")
            .serverHost(HIVEMQ.getHost())
            .serverPort(HIVEMQ.getMqttPort())
            .buildBlocking();
        client.connect();
        try (Mqtt5Publishes publishes = client.publishes(
                com.hivemq.client.mqtt.mqtt5.message.publish.MqttGlobalPublishFilter.ALL)) {
            client.subscribeWith().topicFilter("it/smoke").send();
            client.publishWith().topic("it/smoke")
                .payload("ping".getBytes(StandardCharsets.UTF_8)).send();

            Mqtt5Publish received = publishes.receive(10, TimeUnit.SECONDS);

            assertNotNull(received);
            assertEquals("ping", new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8));
        } finally {
            client.disconnect();
        }
    }
}
```

**Step 2: Run it**

Run: `mvn test -Dtest=MqttBridgeIntegrationTest`
Expected: PASS (first run pulls the ~200 MB `hivemq/hivemq-ce:2026.5` image; subsequent runs are fast).

**Step 3: Commit**

```bash
git add src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java
git commit -m "Add HiveMQ testcontainer smoke test"
```

---

### Task 3: Test scaffolding + happy-path pipeline test

**Files:**
- Modify: `src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java`

**Step 1: Add shared scaffolding**

Replace the smoke test's ad-hoc client with class-level helpers (keep the smoke test, adapted to the helpers where trivial). Add imports: `com.fasterxml.jackson.databind.ObjectMapper`, `com.tencent.cloud.mqtt.model.Connector`, `ConnectorType`, `com.tencent.cloud.mqtt.mqtt.MqttSink`, `MqttSource`, `org.junit.jupiter.api.AfterEach`, `java.util.Optional`.

```java
private static final ObjectMapper MAPPER = new ObjectMapper();
private static final long RECEIVE_TIMEOUT_SECONDS = 10;
private static final long NEGATIVE_WINDOW_MILLIS = 3_000;

private Thread taskThread;
private Mqtt5BlockingClient verifier;

private Connector connector() {
    Connector c = new Connector();
    c.setId("it");
    c.setType(ConnectorType.MQTT);
    c.setAccessPoint(HIVEMQ.getHost() + ":" + HIVEMQ.getMqttPort());
    return c;
}

/** Starts a real Task on a virtual thread. MqttSource subscribes in its constructor, so the subscription is live when this returns. */
private void startTask(String name, String inFilter, String sql, String outTopic, int maxHops) {
    Task task = new Task(name,
        new MqttSource(connector(), inFilter, "source-" + name),
        new SQLTransform<>(sql),
        new MqttSink(connector(), outTopic, "sink-" + name),
        maxHops);
    taskThread = Thread.ofVirtual().start(() -> {
        try {
            task.launch();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    });
}

/** Verifier client subscribed to the given output topic; returns the publishes handle (caller closes). */
private Mqtt5Publishes subscribeVerifier(String clientId, String outTopic) {
    verifier = MqttClient.builder()
        .useMqttVersion5()
        .identifier(clientId)
        .serverHost(HIVEMQ.getHost())
        .serverPort(HIVEMQ.getMqttPort())
        .buildBlocking();
    verifier.connect();
    Mqtt5Publishes publishes = verifier.publishes(
        com.hivemq.client.mqtt.mqtt5.message.publish.MqttGlobalPublishFilter.ALL);
    verifier.subscribeWith().topicFilter(outTopic).send();
    return publishes;
}

private void publish(String topic, String payload, String... userProperties) {
    var builder = verifier.publishWith().topic(topic)
        .payload(payload.getBytes(StandardCharsets.UTF_8));
    if (userProperties.length > 0) {
        var props = com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties.builder();
        for (int i = 0; i < userProperties.length; i += 2) {
            props.add(userProperties[i], userProperties[i + 1]);
        }
        builder.userProperties(props.build());
    }
    builder.send();
}

private static Optional<String> userProperty(Mqtt5Publish publish, String name) {
    return publish.getUserProperties().asList().stream()
        .filter(p -> p.getName().toString().equals(name))
        .map(p -> p.getValue().toString())
        .findFirst();
}

@AfterEach
void tearDown() throws Exception {
    if (taskThread != null) {
        taskThread.interrupt();
        taskThread.join(10_000);
    }
    if (verifier != null) {
        verifier.disconnect();
    }
}
```

(The `publishes` handle is closed implicitly when the verifier disconnects; keeping the handle open for the test duration is fine.)

**Step 2: Write the happy-path test**

```java
@Test
void transformsAndPublishesMatchingRecord() throws Exception {
    startTask("happy-0", "it/happy/in",
        "SELECT payload.device AS device, payload.temp * 2 AS doubled FROM payload WHERE payload.temp > 20",
        "it/happy/out", 1);
    Mqtt5Publishes publishes = subscribeVerifier("verifier-happy", "it/happy/out");

    publish("it/happy/in", "{\"device\":\"d1\",\"temp\":25}",
        "origin", "it", "$__messageId", "msg-happy-1");

    Mqtt5Publish received = publishes.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertNotNull(received, "expected transformed message on sink topic");
    assertEquals(
        MAPPER.readTree("{\"device\":\"d1\",\"doubled\":50}"),
        MAPPER.readTree(new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8)));
    assertEquals(Optional.of("1"), userProperty(received, Task.HOP_COUNT_HEADER));
    assertEquals(Optional.of("it"), userProperty(received, "origin"));
}
```

Note for the implementer: `$__messageId` sets the record key but is intentionally dropped on re-publish (`$__` broker-prefix rule in `MqttRecordMapper.toPublish`), so it is NOT asserted on the output.

**Step 3: Run**

Run: `mvn test -Dtest=MqttBridgeIntegrationTest`
Expected: PASS (both tests).

**Step 4: Commit**

```bash
git add src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java
git commit -m "Add MQTT bridge happy-path integration test"
```

---

### Task 4: Filtered-out test

Publish a non-matching message AND a matching one; assert only the matching one arrives. This proves the pipeline was live (non-vacuous negative).

**Files:**
- Modify: `src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java`

**Step 1: Write the test**

```java
@Test
void dropsRecordFilteredOutBySql() throws Exception {
    startTask("filter-0", "it/filter/in",
        "SELECT * FROM payload WHERE payload.temp > 20",
        "it/filter/out", 1);
    Mqtt5Publishes publishes = subscribeVerifier("verifier-filter", "it/filter/out");

    publish("it/filter/in", "{\"device\":\"cold\",\"temp\":5}");
    publish("it/filter/in", "{\"device\":\"hot\",\"temp\":25}");

    Mqtt5Publish received = publishes.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertNotNull(received, "expected the matching message");
    String body = new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8);
    assertTrue(body.contains("hot"), "only the matching record should be republished, got: " + body);
    assertNull(publishes.receive(NEGATIVE_WINDOW_MILLIS, TimeUnit.MILLISECONDS),
        "filtered record must not reach the sink");
}
```

Add `assertTrue`/`assertNull` to the static imports if missing.

**Step 2: Run**

Run: `mvn test -Dtest=MqttBridgeIntegrationTest`
Expected: PASS (all tests).

**Step 3: Commit**

```bash
git add src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java
git commit -m "Add filtered-record integration test"
```

---

### Task 5: Loop-prevention test

A record arriving with `bridge-hop-count` above `max_hops` is acked and dropped. Publish one over-limit message and one fresh message; assert only the fresh one arrives, with hop count incremented to 1.

**Files:**
- Modify: `src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java`

**Step 1: Write the test**

```java
@Test
void dropsRecordExceedingMaxHops() throws Exception {
    startTask("loop-0", "it/loop/in",
        "SELECT * FROM payload",
        "it/loop/out", 1);
    Mqtt5Publishes publishes = subscribeVerifier("verifier-loop", "it/loop/out");

    publish("it/loop/in", "{\"marker\":\"over-limit\"}",
        Task.HOP_COUNT_HEADER, "2");
    publish("it/loop/in", "{\"marker\":\"fresh\"}");

    Mqtt5Publish received = publishes.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertNotNull(received, "expected the fresh message");
    String body = new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8);
    assertTrue(body.contains("fresh"), "over-limit record must be dropped, got: " + body);
    assertEquals(Optional.of("1"), userProperty(received, Task.HOP_COUNT_HEADER));
    assertNull(publishes.receive(NEGATIVE_WINDOW_MILLIS, TimeUnit.MILLISECONDS),
        "over-limit record must not be republished");
}
```

**Step 2: Run**

Run: `mvn test -Dtest=MqttBridgeIntegrationTest`
Expected: PASS.

**Step 3: Commit**

```bash
git add src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java
git commit -m "Add max_hops loop-prevention integration test"
```

---

### Task 6: Malformed-JSON test

`SQLTransform` drops malformed JSON (logged) without killing the task lane. Publish garbage followed by a valid message; assert the valid one still arrives.

**Files:**
- Modify: `src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java`

**Step 1: Write the test**

```java
@Test
void dropsMalformedJsonAndKeepsProcessing() throws Exception {
    startTask("badjson-0", "it/badjson/in",
        "SELECT * FROM payload WHERE payload.temp > 20",
        "it/badjson/out", 1);
    Mqtt5Publishes publishes = subscribeVerifier("verifier-badjson", "it/badjson/out");

    publish("it/badjson/in", "this is not json");
    publish("it/badjson/in", "{\"device\":\"d2\",\"temp\":30}");

    Mqtt5Publish received = publishes.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    assertNotNull(received, "task lane must survive a malformed record");
    String body = new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8);
    assertTrue(body.contains("d2"), "expected the valid record after the malformed one, got: " + body);
    assertNull(publishes.receive(NEGATIVE_WINDOW_MILLIS, TimeUnit.MILLISECONDS),
        "malformed record must not reach the sink");
}
```

**Step 2: Run**

Run: `mvn test -Dtest=MqttBridgeIntegrationTest`
Expected: PASS.

**Step 3: Commit**

```bash
git add src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java
git commit -m "Add malformed-JSON integration test"
```

---

### Task 7: Full suite + docs touch-up

**Step 1: Run the whole suite**

Run: `mvn test`
Expected: PASS — all existing unit tests plus the 5 integration tests.

**Step 2: Update the Tests line in `CODEBUDDY.md`**

Change the `Tests:` bullet to mention that `MqttBridgeIntegrationTest` requires Docker (it boots a HiveMQ container). One sentence, e.g.:

`- Tests: \`mvn test\` (JUnit 5 + Surefire); \`MqttBridgeIntegrationTest\` boots a throwaway HiveMQ broker via Testcontainers and requires a local Docker daemon; run a single test with \`mvn test -Dtest=SQLTransformTest\``

**Step 3: Commit**

```bash
git add CODEBUDDY.md
git commit -m "Document Docker requirement for integration tests"
```
