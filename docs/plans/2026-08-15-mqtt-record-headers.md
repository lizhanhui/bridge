# MQTT ↔ Record Header Mapping Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Preserve MQTT protocol info and user properties through the bridge by mapping them to/from Record headers, per the design amendment in `docs/plans/2026-08-15-mqtt-source-sink-design.md` (read the "Amendment (2026-08-15)" section first).

**Architecture:** A shared `mqtt.MqttRecordMapper` converts `Mqtt5Publish → Record<String,String>` (source) and `Record → Mqtt5Publish` (sink, headers override global config). `MqttSource` and `MqttSink` delegate to it.

**Tech Stack:** Java 25, hivemq-mqtt-client 1.3.17, kafka-streams `Record` + `org.apache.kafka.common.header.Headers`, JUnit 5, Maven.

---

### Task 1: MqttRecordMapper

**Files:**
- Create: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java`
- Test: `src/test/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapperTest.java`

**Header name constants** (public static final String, on the mapper):
`mqtt.topic` (H_TOPIC), `mqtt.qos` (H_QOS), `mqtt.retained` (H_RETAINED), `mqtt.duplicate` (H_DUPLICATE), `mqtt.message.packet.id` (H_PACKET_ID — defined but never populated), `mqtt.content.type` (H_CONTENT_TYPE), `mqtt.correlation.data` (H_CORRELATION_DATA), `mqtt.response.topic` (H_RESPONSE_TOPIC), `mqtt.message.expiry.interval` (H_MESSAGE_EXPIRY_INTERVAL). Plus `$__messageId` (USER_PROPERTY_MESSAGE_ID) and the `$__` prefix constant (BROKER_PROPERTY_PREFIX). All header string values are UTF-8 encoded.

**Step 1: Write the failing tests**

Source direction — `toRecord(Mqtt5Publish)`:

```java
@Test
void mapsPublishToRecordPreservingProtocolInfoAndUserProperties() {
    Mqtt5Publish publish = Mqtt5Publish.builder()
        .topic("home/room/1")
        .qos(MqttQos.AT_LEAST_ONCE)
        .retain(true)
        .contentType("application/json")
        .correlationData(new byte[]{1, 2, 3})
        .responseTopic("reply/topic")
        .messageExpiryInterval(60)
        .payload("{\"age\":20}".getBytes(StandardCharsets.UTF_8))
        .userProperty("$__messageId", "msg-123")
        .userProperty("custom", "v")
        .build();

    Record<String, String> record = MqttRecordMapper.toRecord(publish);

    assertEquals("msg-123", record.key());                      // key from $__messageId
    assertEquals("{\"age\":20}", record.value());               // payload direct copy
    Headers h = record.headers();
    assertEquals("home/room/1", stringHeader(h, "mqtt.topic"));
    assertEquals("1", stringHeader(h, "mqtt.qos"));
    assertEquals("true", stringHeader(h, "mqtt.retained"));
    assertEquals("false", stringHeader(h, "mqtt.duplicate"));
    assertEquals("application/json", stringHeader(h, "mqtt.content.type"));
    assertArrayEquals(new byte[]{1, 2, 3}, h.lastHeader("mqtt.correlation.data").value());
    assertEquals("reply/topic", stringHeader(h, "mqtt.response.topic"));
    assertEquals("60", stringHeader(h, "mqtt.message.expiry.interval"));
    assertEquals("msg-123", stringHeader(h, "$__messageId"));   // user props round-trip as headers
    assertEquals("v", stringHeader(h, "custom"));
}

@Test
void keyIsNullWhenMessageIdUserPropertyAbsent() {
    Mqtt5Publish publish = Mqtt5Publish.builder()
        .topic("t").payload(new byte[0]).build();
    assertNull(MqttRecordMapper.toRecord(publish).key());
}
```

Sink direction — `toPublish(Record, String defaultTopic, MqttQos defaultQos)`:

```java
@Test
void headersOverrideConfiguredTopicAndQos() {
    Record<String, String> record = new Record<>("k", "v", 1L, new RecordHeaders()
        .add("mqtt.topic", utf8("override/topic"))
        .add("mqtt.qos", utf8("0"))
        .add("mqtt.retained", utf8("true")));

    Mqtt5Publish publish = MqttRecordMapper.toPublish(record, "default/topic", MqttQos.AT_LEAST_ONCE);

    assertEquals("override/topic", publish.getTopic().toString());
    assertEquals(MqttQos.AT_MOST_ONCE, publish.getQos());
    assertTrue(publish.isRetain());
}

@Test
void defaultsUsedWhenHeadersAbsent() {
    Record<String, String> record = new Record<>("k", "v", 1L);
    Mqtt5Publish publish = MqttRecordMapper.toPublish(record, "default/topic", MqttQos.AT_LEAST_ONCE);
    assertEquals("default/topic", publish.getTopic().toString());
    assertEquals(MqttQos.AT_LEAST_ONCE, publish.getQos());
    assertFalse(publish.isRetain());
}

@Test
void userPropertyHeadersRoundTripButReservedAndBrokerHeadersAreExcluded() {
    Record<String, String> record = new Record<>("k", "v", 1L, new RecordHeaders()
        .add("mqtt.topic", utf8("t"))
        .add("mqtt.duplicate", utf8("true"))
        .add("mqtt.message.packet.id", utf8("7"))
        .add("$__messageId", utf8("msg-123"))
        .add("$__publisherClientId", utf8("client-9"))
        .add("custom", utf8("v")));

    Mqtt5Publish publish = MqttRecordMapper.toPublish(record, "default", MqttQos.AT_LEAST_ONCE);

    List<MqttUserProperty> props = publish.getUserProperties().asList();
    assertEquals(1, props.size());
    assertEquals("custom", props.get(0).getName().toString());
    assertEquals("v", props.get(0).getValue().toString());
}

@Test
void malformedQosHeaderFallsBackToDefault() {
    Record<String, String> record = new Record<>("k", "v", 1L, new RecordHeaders()
        .add("mqtt.qos", utf8("banana")));
    Mqtt5Publish publish = MqttRecordMapper.toPublish(record, "default", MqttQos.AT_LEAST_ONCE);
    assertEquals(MqttQos.AT_LEAST_ONCE, publish.getQos());
}
```

(Helper `stringHeader(Headers, String)` decodes `lastHeader(name).value()` as UTF-8; `utf8(String)` encodes.)

**Step 2: Run tests to verify they fail**

Run: `mvn test -Dtest=MqttRecordMapperTest`
Expected: compilation failure — class does not exist.

**Step 3: Implement**

```java
package com.tencent.cloud.mqtt.mqtt;

// Source direction: toRecord(Mqtt5Publish)
//   - headers: mqtt.topic, mqtt.qos (code as string), mqtt.retained, mqtt.duplicate,
//     mqtt.content.type / mqtt.correlation.data (raw bytes, duplicate() the ByteBuffer
//     before reading) / mqtt.response.topic / mqtt.message.expiry.interval — optional ones
//     only when present
//   - every user property → header with same name (UTF-8)
//   - key = value of $__messageId user property, else null
//   - timestamp = System.currentTimeMillis()
//   - value = payload bytes as UTF-8 string
//   - build headers with org.apache.kafka.common.header.internals.RecordHeaders
//
// Sink direction: toPublish(Record<String,String>, String defaultTopic, MqttQos defaultQos)
//   - Mqtt5Publish.builder(): topic = mqtt.topic header or defaultTopic;
//     qos = parse mqtt.qos header (fromCode; on NumberFormatException or unknown code,
//     log.warn and use defaultQos); retain = parse mqtt.retained ("true" → true);
//     contentType / correlationData (ByteBuffer.wrap) / responseTopic /
//     messageExpiryInterval (long seconds) from their headers when present
//   - userProperty(name, value) for every header that is NOT in the reserved set
//     {mqtt.topic, mqtt.qos, mqtt.retained, mqtt.duplicate, mqtt.message.packet.id,
//      mqtt.content.type, mqtt.correlation.data, mqtt.response.topic,
//      mqtt.message.expiry.interval} and does NOT start with "$__"
//   - payload = record value UTF-8 bytes
```

Implementation notes: `Mqtt5Publish` getters — `getTopic()`, `getQos()`, `isRetain()`, `isDuplicate()`, `getContentType()` (Optional), `getCorrelationData()` (Optional<ByteBuffer>), `getResponseTopic()` (Optional), `getMessageExpiryInterval()` (OptionalLong), `getUserProperties().asList()`, `getPayloadAsBytes()`. `MqttQos.fromCode(int)` — verify the exact lookup method against 1.3.17 via javap. `Mqtt5Publish.builder().userProperty(String, String)` exists. Record constructor with headers: `new Record<>(key, value, timestamp, headers)`.

**Step 4: Run tests to verify they pass**

Run: `mvn test`
Expected: PASS (new 6 tests + existing 13).

**Step 5: Commit**

```bash
git add src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java src/test/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapperTest.java
git commit -m "Add MQTT<->Record header mapper preserving protocol info and user properties"
```

---

### Task 2: Wire mapper into MqttSource and MqttSink

**Files:**
- Modify: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSource.java` (callback)
- Modify: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSink.java` (publish)

**Step 1: MqttSource**

Replace the manual `new Record<>(...)` construction in the subscribe callback with `MqttRecordMapper.toRecord(publish)`. Everything else (queue offer, timeout, drop logging, interrupt handling) unchanged.

**Step 2: MqttSink**

Replace the fluent `publishWith()` call with:

```java
Mqtt5Publish publish = MqttRecordMapper.toPublish(record, topic, MqttQos.AT_LEAST_ONCE);
client.toBlocking().publish(publish);
```

Keep the existing try/catch error logging and fail-fast rethrow. The configured `topic` field stays as the default the mapper falls back to.

**Step 3: Verify**

Run: `mvn test`
Expected: BUILD SUCCESS, all 19 tests pass.

**Step 4: Commit**

```bash
git add -A
git commit -m "Use header-preserving record mapping in MQTT source and sink"
```
