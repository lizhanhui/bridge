package com.tencent.cloud.mqtt.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

class KafkaRecordMapperTest {
    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void preservesKeyValueTimestampWithNoExplicitPartition() {
        ProducerRecord<String, String> output = KafkaRecordMapper.toProducerRecord(
            new Record<>("key", "value", 123L), "configured-topic");
        assertEquals("configured-topic", output.topic());
        assertEquals("key", output.key());
        assertEquals("value", output.value());
        assertEquals(123L, output.timestamp());
        assertNull(output.partition());
        assertEquals(0, output.headers().toArray().length);
    }

    @Test
    void supportsNullKeyAndTombstoneValue() {
        ProducerRecord<String, String> output = KafkaRecordMapper.toProducerRecord(
            new Record<>(null, null, 1L), "out");
        assertNull(output.key());
        assertNull(output.value());
    }

    @Test
    void lastRoutingOverrideWinsAndAllKafkaRoutingHeadersAreConsumed() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("dst.kafka.topic", bytes("first"));
        headers.add("keep", bytes("metadata"));
        headers.add("dst.kafka.topic", bytes("last"));
        Record<String, String> input = new Record<>("key", "value", 1L, headers);
        ProducerRecord<String, String> output = KafkaRecordMapper.toProducerRecord(input, "default");
        assertEquals("last", output.topic());
        assertNull(output.headers().lastHeader("dst.kafka.topic"));
        assertEquals(3, input.headers().toArray().length);
        assertEquals("last", new String(input.headers().lastHeader("dst.kafka.topic").value(),
            StandardCharsets.UTF_8));
    }

    @Test
    void nullLastOverrideFallsBackToConfiguredTopic() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("dst.kafka.topic", bytes("first"));
        headers.add("dst.kafka.topic", null);
        ProducerRecord<String, String> output = KafkaRecordMapper.toProducerRecord(
            new Record<>("key", "value", 1L, headers), "default");
        assertEquals("default", output.topic());
        assertNull(output.headers().lastHeader("dst.kafka.topic"));
    }

    @Test
    void retainsOrderedDuplicateBinaryNullAndCrossProtocolMetadataWithoutAliasing() {
        RecordHeaders headers = new RecordHeaders();
        List<String> keys = List.of("src.mqtt.topic", "bridge-hop-count", "mqtt.qos",
            "rmq.tag", "$__trace", "dst.mqtt.topic", "dst.rmq.topic", "duplicate", "duplicate", "binary", "null");
        for (int index = 0; index < keys.size(); index++) {
            headers.add(keys.get(index), index == keys.size() - 1 ? null : new byte[] {(byte) index, 0, (byte) 255});
        }
        Record<String, String> input = new Record<>("key", "value", 1L, headers);
        ProducerRecord<String, String> output = KafkaRecordMapper.toProducerRecord(input, "out");
        assertEquals(keys, StreamSupport.stream(output.headers().spliterator(), false).map(Header::key).toList());
        Header[] sourceHeaders = input.headers().toArray();
        Header[] destinationHeaders = output.headers().toArray();
        for (int index = 0; index < keys.size(); index++) {
            assertArrayEquals(sourceHeaders[index].value(), destinationHeaders[index].value());
            assertNotSame(sourceHeaders[index], destinationHeaders[index]);
            if (sourceHeaders[index].value() != null) {
                assertNotSame(sourceHeaders[index].value(), destinationHeaders[index].value());
            }
        }
        destinationHeaders[0].value()[0] = 99;
        assertEquals(0, sourceHeaders[0].value()[0]);
        output.headers().remove("duplicate");
        assertEquals(2, StreamSupport.stream(input.headers().headers("duplicate").spliterator(), false).count());
    }

    @Test
    void rejectsInvalidOverrideTopicsAsRecordPoison() {
        for (String topic : List.of("", " ", ".", "..", "bad/topic", "non-ascii-é", "x".repeat(250))) {
            RecordHeaders headers = new RecordHeaders();
            headers.add("dst.kafka.topic", bytes(topic));
            assertThrows(IllegalArgumentException.class, () -> KafkaRecordMapper.toProducerRecord(
                new Record<>("key", "value", 1L, headers), "valid"));
        }
    }

    @Test
    void supportsValidKafkaTopicCharactersAndMaximumLength() {
        for (String topic : List.of("a.b_c-9", "x".repeat(249))) {
            assertEquals(topic, KafkaRecordMapper.toProducerRecord(new Record<>("k", "v", 1L), topic).topic());
        }
    }
}
