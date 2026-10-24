package com.tencent.cloud.mqtt.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

class KafkaRecordMapperTest {
    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static ConsumerRecord<String, String> consumed(String key, String value,
            long timestamp, RecordHeaders headers) {
        return new ConsumerRecord<>("input-topic", 3, 42L, timestamp, TimestampType.CREATE_TIME,
            ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, key, value, headers, Optional.empty());
    }

    @Test
    void inboundPreservesKeyValueAndBrokerTimestamp() {
        Record<String, String> output = KafkaRecordMapper.toRecord(
            consumed("key", "value", 123L, new RecordHeaders()));
        assertEquals("key", output.key());
        assertEquals("value", output.value());
        assertEquals(123L, output.timestamp());
        assertEquals("src.kafka.topic", KafkaRecordMapper.H_SRC_TOPIC);
        assertEquals("kafka.partition", KafkaRecordMapper.H_PARTITION);
        assertEquals("kafka.offset", KafkaRecordMapper.H_OFFSET);
        assertArrayEquals(bytes("input-topic"), output.headers().lastHeader(KafkaRecordMapper.H_SRC_TOPIC).value());
        assertArrayEquals(bytes("3"), output.headers().lastHeader(KafkaRecordMapper.H_PARTITION).value());
        assertArrayEquals(bytes("42"), output.headers().lastHeader(KafkaRecordMapper.H_OFFSET).value());
    }

    @Test
    void inboundPreservesNullKeyTombstoneAndZeroTimestamp() {
        Record<String, String> output = KafkaRecordMapper.toRecord(
            consumed(null, null, 0L, new RecordHeaders()));
        assertNull(output.key());
        assertNull(output.value());
        assertEquals(0L, output.timestamp());
    }

    @Test
    void inboundNoTimestampUsesReceiveTimeWithinCallBounds() {
        ConsumerRecord<String, String> input = new ConsumerRecord<>("input-topic", 3, 42L, "key", "value");
        assertEquals(ConsumerRecord.NO_TIMESTAMP, input.timestamp());
        long before = System.currentTimeMillis();
        Record<String, String> output = KafkaRecordMapper.toRecord(input);
        long after = System.currentTimeMillis();
        assertTrue(output.timestamp() >= before);
        assertTrue(output.timestamp() <= after);
    }

    @Test
    void inboundAnyNegativeTimestampUsesReceiveTime() {
        long before = System.currentTimeMillis();
        Record<String, String> output = KafkaRecordMapper.toRecord(
            consumed("key", "value", -2L, new RecordHeaders()));
        long after = System.currentTimeMillis();
        assertTrue(output.timestamp() >= before);
        assertTrue(output.timestamp() <= after);
    }

    @Test
    void inboundRealMetadataReplacesEveryUpstreamReservedHeader() {
        RecordHeaders headers = new RecordHeaders();
        for (String key : List.of("src.kafka.topic", "kafka.partition", "kafka.offset")) {
            headers.add(key, bytes("spoofed"));
            headers.add("keep-" + key, bytes("retain"));
            headers.add(key, null);
        }
        ConsumerRecord<String, String> input = consumed("key", "value", 1L, headers);
        Record<String, String> output = KafkaRecordMapper.toRecord(input);
        assertEquals(List.of("keep-src.kafka.topic", "keep-kafka.partition", "keep-kafka.offset",
            "src.kafka.topic", "kafka.partition", "kafka.offset"),
            StreamSupport.stream(output.headers().spliterator(), false).map(Header::key).toList());
        for (String key : List.of("src.kafka.topic", "kafka.partition", "kafka.offset")) {
            assertEquals(1, StreamSupport.stream(output.headers().headers(key).spliterator(), false).count());
            assertEquals(2, StreamSupport.stream(input.headers().headers(key).spliterator(), false).count());
        }
        assertArrayEquals(bytes("input-topic"), output.headers().lastHeader("src.kafka.topic").value());
        assertArrayEquals(bytes("3"), output.headers().lastHeader("kafka.partition").value());
        assertArrayEquals(bytes("42"), output.headers().lastHeader("kafka.offset").value());
    }

    @Test
    void inboundRetainsNarrowReservedSetOrderedDuplicateBinaryNullAndCrossProtocolHeadersWithoutAliasing() {
        RecordHeaders headers = new RecordHeaders();
        List<String> keys = List.of("src.mqtt.topic", "src.rmq.topic", "bridge-hop-count",
            "mqtt.qos", "rmq.tag", "$__trace", "dst.mqtt.topic", "dst.rmq.topic",
            "dst.kafka.topic", "kafka.custom", "kafka.partition.extra", "duplicate", "duplicate",
            "binary", "null");
        for (int index = 0; index < keys.size(); index++) {
            headers.add(keys.get(index), index == keys.size() - 1 ? null : new byte[] {(byte) index, 0, (byte) 255});
        }
        ConsumerRecord<String, String> input = consumed("key", "value", 1L, headers);
        Record<String, String> output = KafkaRecordMapper.toRecord(input);
        assertEquals(keys, StreamSupport.stream(output.headers().spliterator(), false)
            .limit(keys.size()).map(Header::key).toList());
        assertEquals(keys.size() + 3, output.headers().toArray().length);
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
        sourceHeaders[1].value()[0] = 88;
        assertEquals(1, destinationHeaders[1].value()[0]);
        output.headers().remove("duplicate");
        assertEquals(2, StreamSupport.stream(input.headers().headers("duplicate").spliterator(), false).count());
    }

    @Test
    void inboundPartitionAndOffsetMetadataDoNotControlOutboundRouting() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("dst.kafka.topic", bytes("routed-topic"));
        Record<String, String> mapped = KafkaRecordMapper.toRecord(consumed("key", "value", 1L, headers));
        ProducerRecord<String, String> output = KafkaRecordMapper.toProducerRecord(mapped, "default");
        assertEquals("routed-topic", output.topic());
        assertNull(output.partition());
        assertArrayEquals(bytes("3"), output.headers().lastHeader("kafka.partition").value());
        assertArrayEquals(bytes("42"), output.headers().lastHeader("kafka.offset").value());
        assertNull(output.headers().lastHeader("dst.kafka.topic"));
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
