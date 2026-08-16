package com.tencent.cloud.mqtt.mqtt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

class MqttRecordMapperTest {

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
            .userProperties(Mqtt5UserProperties.of(
                Mqtt5UserProperty.of("$__messageId", "msg-123"),
                Mqtt5UserProperty.of("custom", "v")))
            .build();

        Record<String, String> record = MqttRecordMapper.toRecord(publish);

        assertEquals("msg-123", record.key());
        assertEquals("{\"age\":20}", record.value());
        Headers h = record.headers();
        assertEquals("home/room/1", stringHeader(h, "src.mqtt.topic"));
        assertEquals("1", stringHeader(h, "mqtt.qos"));
        assertEquals("true", stringHeader(h, "mqtt.retained"));
        assertEquals("false", stringHeader(h, "mqtt.duplicate"));
        assertEquals("application/json", stringHeader(h, "mqtt.content.type"));
        assertArrayEquals(new byte[]{1, 2, 3}, h.lastHeader("mqtt.correlation.data").value());
        assertEquals("reply/topic", stringHeader(h, "mqtt.response.topic"));
        assertEquals("60", stringHeader(h, "mqtt.message.expiry.interval"));
        assertEquals("msg-123", stringHeader(h, "$__messageId"));
        assertEquals("v", stringHeader(h, "custom"));
    }

    @Test
    void keyIsNullWhenMessageIdUserPropertyAbsent() {
        Mqtt5Publish publish = Mqtt5Publish.builder()
            .topic("t").payload(new byte[0]).build();
        assertNull(MqttRecordMapper.toRecord(publish).key());
    }

    @Test
    void dstTopicHeaderOverridesConfiguredTopicAndQos() {
        Record<String, String> record = new Record<>("k", "v", 1L, new RecordHeaders()
            .add("dst.mqtt.topic", utf8("override/topic"))
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
            .add("src.mqtt.topic", utf8("home/in"))
            .add("dst.mqtt.topic", utf8("override/out"))
            .add("mqtt.duplicate", utf8("true"))
            .add("mqtt.message.packet.id", utf8("7"))
            .add("$__messageId", utf8("msg-123"))
            .add("$__publisherClientId", utf8("client-9"))
            .add("custom", utf8("v")));

        Mqtt5Publish publish = MqttRecordMapper.toPublish(record, "default", MqttQos.AT_LEAST_ONCE);

        assertEquals("override/out", publish.getTopic().toString());
        List<? extends Mqtt5UserProperty> props = publish.getUserProperties().asList();
        assertEquals(2, props.size());
        assertEquals("src.mqtt.topic", props.get(0).getName().toString());
        assertEquals("home/in", props.get(0).getValue().toString());
        assertEquals("custom", props.get(1).getName().toString());
        assertEquals("v", props.get(1).getValue().toString());
    }

    @Test
    void malformedQosHeaderFallsBackToDefault() {
        Record<String, String> record = new Record<>("k", "v", 1L, new RecordHeaders()
            .add("mqtt.qos", utf8("banana")));
        Mqtt5Publish publish = MqttRecordMapper.toPublish(record, "default", MqttQos.AT_LEAST_ONCE);
        assertEquals(MqttQos.AT_LEAST_ONCE, publish.getQos());
    }

    @Test
    void userPropertyWithReservedNameDoesNotShadowProtocolHeader() {
        Mqtt5Publish publish = Mqtt5Publish.builder()
            .topic("real/topic")
            .payload(new byte[0])
            .userProperties(Mqtt5UserProperties.of(
                Mqtt5UserProperty.of("src.mqtt.topic", "evil/topic")))
            .build();

        Record<String, String> record = MqttRecordMapper.toRecord(publish);

        Headers h = record.headers();
        assertEquals("real/topic", stringHeader(h, "src.mqtt.topic"));
        int count = 0;
        for (Header ignored : h.headers("src.mqtt.topic")) {
            count++;
        }
        assertEquals(1, count);
    }

    @Test
    void outOfRangeExpiryHeaderFallsBackToUnset() {
        Record<String, String> record = new Record<>("k", "v", 1L, new RecordHeaders()
            .add("mqtt.message.expiry.interval", utf8("5000000000")));
        Mqtt5Publish publish = MqttRecordMapper.toPublish(record, "default", MqttQos.AT_LEAST_ONCE);
        assertFalse(publish.getMessageExpiryInterval().isPresent());
    }

    @Test
    void optionalHeadersAppliedToPublish() {
        Record<String, String> record = new Record<>("k", "v", 1L, new RecordHeaders()
            .add("mqtt.content.type", utf8("application/json"))
            .add("mqtt.correlation.data", new byte[]{4, 5})
            .add("mqtt.response.topic", utf8("reply/here"))
            .add("mqtt.message.expiry.interval", utf8("120")));

        Mqtt5Publish publish = MqttRecordMapper.toPublish(record, "default", MqttQos.AT_LEAST_ONCE);

        assertEquals("application/json", publish.getContentType().orElseThrow().toString());
        ByteBuffer correlationData = publish.getCorrelationData().orElseThrow().duplicate();
        byte[] correlationBytes = new byte[correlationData.remaining()];
        correlationData.get(correlationBytes);
        assertArrayEquals(new byte[]{4, 5}, correlationBytes);
        assertEquals("reply/here", publish.getResponseTopic().orElseThrow().toString());
        assertEquals(120, publish.getMessageExpiryInterval().orElseThrow());
    }

    @Test
    void dstTopicUserPropertyFromPublisherBecomesRoutingHeader() {
        Mqtt5Publish publish = Mqtt5Publish.builder()
            .topic("real/topic")
            .payload(new byte[0])
            .userProperties(Mqtt5UserProperties.of(
                Mqtt5UserProperty.of("dst.mqtt.topic", "reroute/here")))
            .build();

        Record<String, String> record = MqttRecordMapper.toRecord(publish);

        assertEquals("reroute/here", stringHeader(record.headers(), "dst.mqtt.topic"));
        // and it survives to drive the sink topic on publish:
        Mqtt5Publish out = MqttRecordMapper.toPublish(record, "default", MqttQos.AT_LEAST_ONCE);
        assertEquals("reroute/here", out.getTopic().toString());
    }

    private static String stringHeader(Headers headers, String name) {
        return new String(headers.lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
