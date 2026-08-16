# src/dst Topic Header Split Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Split the dual-role `mqtt.topic` / `rmq.topic` record headers into `src.*.topic` (informational, set on consume, propagates downstream) and `dst.*.topic` (deliberate sink-topic override, consumed on produce), symmetric across MQTT and RocketMQ, and revert the `MqttSink` force-override patch.

**Architecture:** Per `docs/plans/2026-08-16-src-dst-topic-headers-design.md`. Each mapper's single `RESERVED_HEADERS` set splits by direction: a consume-shadow set (contains `src.*.topic`) and a publish-consumed set (excludes `src.*.topic` so it propagates; includes `dst.*.topic` so the override is consumed, not propagated).

**Tech Stack:** Java 25, JUnit 5, existing hivemq-mqtt-client / rocketmq-client-java.

**Context for the implementer:**
- `MqttRecordMapper` / `RocketMQRecordMapper` are static utilities; `RESERVED_HEADERS` is currently used BOTH for consume-side shadow protection (skip same-named user properties in `toRecord`) AND publish-side exclusion (skip headers when building user properties/message properties in `toPublish`/`toMessage`). These two uses must diverge.
- TDD applies: update the mapper unit tests FIRST, watch them fail, then implement.
- `MqttSink.publish` currently has a force-override (`80a1268`): `.extend().topic(topic).build()` after `toPublish` — Task 2 removes it.
- Run tests with `mvn test -Dtest=<ClassName>`; full suite before each commit touching shared code.
- Never stage `conf/tasks.json` (pre-existing user change).

---

### Task 1: MqttRecordMapper src/dst split (TDD)

**Files:**
- Modify: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java`
- Test: `src/test/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapperTest.java`

**Step 1: Update the failing tests first**

In `MqttRecordMapperTest.java`:

1. `mapsPublishToRecordPreservingProtocolInfoAndUserProperties` (line 47): change `stringHeader(h, "mqtt.topic")` → `stringHeader(h, "src.mqtt.topic")`.

2. Rename `headersOverrideConfiguredTopicAndQos` → `dstTopicHeaderOverridesConfiguredTopicAndQos`; change the header name at line 69 from `"mqtt.topic"` to `"dst.mqtt.topic"` (assertions unchanged: topic is `override/topic`).

3. `userPropertyHeadersRoundTripButReservedAndBrokerHeadersAreExcluded` (line 90): change the record headers to exercise the new semantics — replace `.add("mqtt.topic", utf8("t"))` with `.add("src.mqtt.topic", utf8("home/in"))` and add `.add("dst.mqtt.topic", utf8("override/out"))`. Change the assertion block: the publish's topic must be `override/out`, and exactly TWO user properties survive — `src.mqtt.topic=home/in` (src propagates) and `custom=v`; `dst.mqtt.topic`, `mqtt.duplicate`, `mqtt.message.packet.id`, and `$__*` are consumed/dropped:

```java
Mqtt5Publish publish = MqttRecordMapper.toPublish(record, "default", MqttQos.AT_LEAST_ONCE);

assertEquals("override/out", publish.getTopic().toString());
List<? extends Mqtt5UserProperty> props = publish.getUserProperties().asList();
assertEquals(2, props.size());
```

(Assert the two surviving name/value pairs explicitly; order follows header insertion order — `src.mqtt.topic` then `custom` — but prefer a content check over index reliance if cleaner.)

4. `userPropertyWithReservedNameDoesNotShadowProtocolHeader` (line 116): the evil user property becomes `"src.mqtt.topic"` (value `"evil/topic"`); assertions change to `stringHeader(h, "src.mqtt.topic")` equals `"real/topic"` and exactly one `src.mqtt.topic` header exists.

**Step 2: Run tests to verify they fail**

Run: `mvn test -Dtest=MqttRecordMapperTest`
Expected: FAIL (compilation error — `src.mqtt.topic` behavior missing / wrong header values).

**Step 3: Implement in `MqttRecordMapper.java`**

- Replace `public static final String H_TOPIC = "mqtt.topic";` with:

```java
/** Informational: the topic the record was consumed from. Propagates downstream. */
public static final String H_SRC_TOPIC = "src.mqtt.topic";
/** Routing override: when present, the sink publishes to this topic instead of its configured one. Consumed on publish. */
public static final String H_DST_TOPIC = "dst.mqtt.topic";
```

- In `RESERVED_HEADERS` (consume-shadow set): replace `H_TOPIC` with `H_SRC_TOPIC`.
- Add a publish-consumed set:

```java
/** Headers consumed as publish settings/overrides — everything reserved on consume EXCEPT
 *  {@value #H_SRC_TOPIC} (which propagates), PLUS {@value #H_DST_TOPIC} (the override). */
private static final Set<String> PUBLISH_CONSUMED_HEADERS = Set.of(
    H_QOS, H_RETAINED, H_DUPLICATE, H_PACKET_ID,
    H_CONTENT_TYPE, H_CORRELATION_DATA, H_RESPONSE_TOPIC, H_MESSAGE_EXPIRY_INTERVAL,
    H_DST_TOPIC);
```

- `toRecord`: `headers.add(H_SRC_TOPIC, utf8(publish.getTopic().toString()));`
- `toPublish`: `.topic(stringHeader(headers, H_DST_TOPIC, defaultTopic))`; the user-property loop condition uses `PUBLISH_CONSUMED_HEADERS` instead of `RESERVED_HEADERS`.
- Update the class and `toPublish` javadoc: protocol info now travels in `mqtt.*` headers plus `src.mqtt.topic`; `dst.mqtt.topic` overrides the sink topic; `src.mqtt.topic` round-trips as a user property.

**Step 4: Run tests to verify they pass**

Run: `mvn test -Dtest=MqttRecordMapperTest`
Expected: PASS (8 tests).

**Step 5: Commit**

```bash
git add src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java src/test/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapperTest.java
git commit -m "Split mqtt.topic header into src.mqtt.topic and dst.mqtt.topic"
```

---

### Task 2: Revert the MqttSink force-override

**Files:**
- Modify: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSink.java:29-38`

**Step 1: Revert to the plain mapping call**

```java
@Override
public void publish(Record<String, String> record) {
    try {
        Mqtt5Publish publish = MqttRecordMapper.toPublish(record, topic, MqttQos.AT_LEAST_ONCE);
        client.toBlocking().publish(publish);
    } catch (RuntimeException e) {
        log.error("Failed to publish to topic {} on {}", topic, accessPoint, e);
        throw e;
    }
}
```

(Remove the `.extend().topic(topic).build()` and its comment. The override now lives in the mapper via `dst.mqtt.topic`; the configured topic is the default again.)

**Step 2: Run the integration test that guards routing**

Run: `mvn test -Dtest=MqttBridgeIntegrationTest`
Expected: PASS — `transformsAndPublishesMatchingRecord` asserts `received.getTopic()` is `it/happy/out`, proving no input-topic echo after the revert.

**Step 3: Commit**

```bash
git add src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSink.java
git commit -m "Revert MqttSink force-topic patch; dst.mqtt.topic now governs overrides"
```

---

### Task 3: RocketMQRecordMapper src/dst split (TDD)

**Files:**
- Modify: `src/main/java/com/tencent/cloud/mqtt/rocketmq/RocketMQRecordMapper.java`
- Test: `src/test/java/com/tencent/cloud/mqtt/rocketmq/RocketMQRecordMapperTest.java`

**Step 1: Update the failing tests first**

In `RocketMQRecordMapperTest.java`:

1. `messageViewMapsToRecord` (line 82): `RocketMQRecordMapper.H_TOPIC` → `RocketMQRecordMapper.H_SRC_TOPIC`.
2. `propertyCollidingWithReservedHeaderIsSkipped` (lines 94-101): the evil property becomes `"src.rmq.topic"`; assertion uses `H_SRC_TOPIC`.
3. Rename `topicHeaderOverridesDefaultTopic` → `dstTopicHeaderOverridesDefaultTopic`; the header added is `RocketMQRecordMapper.H_DST_TOPIC` (value `"other"`); assertions unchanged (topic `"other"`, empty properties — dst is consumed, not propagated).
4. Add a new test `srcTopicHeaderPropagatesAsProperty`:

```java
@Test
void srcTopicHeaderPropagatesAsProperty() {
    Record<String, String> record = new Record<>("k", "{}", 1L);
    record.headers().add(RocketMQRecordMapper.H_SRC_TOPIC,
        "home/in".getBytes(StandardCharsets.UTF_8));

    Message message = RocketMQRecordMapper.toMessage(
        ClientServiceProvider.loadService(), record, "home");

    assertEquals("home", message.getTopic());
    assertEquals(Map.of("src.rmq.topic", "home/in"), message.getProperties());
}
```

**Step 2: Run to verify failure**

Run: `mvn test -Dtest=RocketMQRecordMapperTest`
Expected: FAIL (compilation error — constants don't exist).

**Step 3: Implement in `RocketMQRecordMapper.java`**

Mirror Task 1:
- Replace `H_TOPIC = "rmq.topic"` with `H_SRC_TOPIC = "src.rmq.topic"` and `H_DST_TOPIC = "dst.rmq.topic"` (same javadoc intent as MQTT).
- `RESERVED_HEADERS`: replace `H_TOPIC` with `H_SRC_TOPIC`.
- Add `PUBLISH_CONSUMED_HEADERS` = {H_MESSAGE_ID, H_TAG, H_KEYS, H_BORN_TIMESTAMP, H_DELIVERY_TIMESTAMP, H_DELIVERY_ATTEMPT, H_BORN_HOST, H_DST_TOPIC}.
- `toRecord`: `headers.add(H_SRC_TOPIC, utf8(messageView.getTopic()));`
- `toMessage`: `.setTopic(stringHeader(headers, H_DST_TOPIC, defaultTopic))`; the property loop uses `PUBLISH_CONSUMED_HEADERS`.
- Update class/`toMessage` javadoc (dst override, src propagation).

**Step 4: Run to verify pass**

Run: `mvn test -Dtest=RocketMQRecordMapperTest`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/main/java/com/tencent/cloud/mqtt/rocketmq/RocketMQRecordMapper.java src/test/java/com/tencent/cloud/mqtt/rocketmq/RocketMQRecordMapperTest.java
git commit -m "Split rmq.topic header into src.rmq.topic and dst.rmq.topic"
```

---

### Task 4: Integration tests — provenance + dst reroute

**Files:**
- Modify: `src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java`

**Step 1: Add provenance assertion to the happy path**

In `transformsAndPublishesMatchingRecord`, after the existing user-property assertions add:

```java
assertEquals(Optional.of("it/happy/in"), userProperty(received, "src.mqtt.topic"));
```

**Step 2: Add the dst-override reroute test**

Output topics deliberately live under a different prefix than the input, so the verifier's wildcard subscription can't capture the verifier's own input publish:

```java
@Test
void dstTopicHeaderReroutesOutput() throws Exception {
    startTask("dst-0", "it/dst/in",
        "SELECT * FROM payload",
        "it/dst-out/configured", 1);
    Mqtt5Publishes publishes = subscribeVerifier("verifier-dst", "it/dst-out/+");

    publish("it/dst/in", "{\"marker\":\"reroute\"}",
        "dst.mqtt.topic", "it/dst-out/override");

    Mqtt5Publish received = publishes.receive(RECEIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS).orElse(null);
    assertNotNull(received, "expected rerouted message on the dst.mqtt.topic topic");
    assertEquals("it/dst-out/override", received.getTopic().toString());
    String body = new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8);
    assertTrue(body.contains("reroute"), "unexpected payload: " + body);
    assertEquals(Optional.of("it/dst/in"), userProperty(received, "src.mqtt.topic"));
    assertEquals(Optional.empty(), userProperty(received, "dst.mqtt.topic"));
    assertNull(publishes.receive(NEGATIVE_WINDOW_MILLIS, TimeUnit.MILLISECONDS).orElse(null),
        "nothing should arrive on the sink's configured topic");
}
```

(`Optional.empty()` needs no new import — `Optional` is already imported. `assertEquals(Optional.empty(), ...)` pins that the dst header is consumed, not propagated.)

**Step 3: Run**

Run: `mvn test -Dtest=MqttBridgeIntegrationTest`
Expected: PASS (6 tests).

**Step 4: Commit**

```bash
git add src/test/java/com/tencent/cloud/mqtt/MqttBridgeIntegrationTest.java
git commit -m "Add src/dst topic integration coverage: provenance and reroute"
```

---

### Task 5: Docs + full suite

**Step 1: Update the header-mapping reference doc**

`docs/plans/2026-08-15-mqtt-record-headers.md` documents the old mapping (see lines ~20, ~47, ~73, ~96, ~133-150: header lists and test snippets referencing `mqtt.topic`). Update the narrative sections to the new model: `src.mqtt.topic` (consume-set, propagates), `dst.mqtt.topic` (publish override, consumed), remaining `mqtt.*` headers unchanged. Refresh inline test snippets to match the new test code. Do not rewrite the doc's structure — minimal accurate edits.

**Step 2: Update CODEBUDDY.md mapper descriptions**

In the Architecture bullet for `com.tencent.cloud.mqtt.mqtt` (line ~31) and `rocketmq` (line ~33): note that source/destination topics travel as `src.*.topic` (propagating) / `dst.*.topic` (override) headers. One clause each.

**Step 3: Full suite**

Run: `mvn test`
Expected: PASS — all unit + 6 integration tests.

**Step 4: Commit**

```bash
git add docs/plans/2026-08-15-mqtt-record-headers.md CODEBUDDY.md
git commit -m "Document src/dst topic header semantics"
```
