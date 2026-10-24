package com.tencent.cloud.mqtt.kafka;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;

public final class KafkaRecordMapper {
    public static final String H_DST_TOPIC = "dst.kafka.topic";
    public static final String H_SRC_TOPIC = "src.kafka.topic";
    public static final String H_PARTITION = "kafka.partition";
    public static final String H_OFFSET = "kafka.offset";

    private KafkaRecordMapper() {}

    public static Record<String, String> toRecord(ConsumerRecord<String, String> record) {
        RecordHeaders headers = new RecordHeaders();
        for (Header header : record.headers()) {
            if (!H_SRC_TOPIC.equals(header.key()) && !H_PARTITION.equals(header.key())
                    && !H_OFFSET.equals(header.key())) {
                byte[] value = header.value();
                headers.add(header.key(), value == null ? null : value.clone());
            }
        }
        headers.add(H_SRC_TOPIC, record.topic().getBytes(StandardCharsets.UTF_8));
        headers.add(H_PARTITION, Integer.toString(record.partition()).getBytes(StandardCharsets.UTF_8));
        headers.add(H_OFFSET, Long.toString(record.offset()).getBytes(StandardCharsets.UTF_8));
        long timestamp = record.timestamp() >= 0 ? record.timestamp() : System.currentTimeMillis();
        return new Record<>(record.key(), record.value(), timestamp, headers);
    }

    public static ProducerRecord<String, String> toProducerRecord(Record<String, String> record, String defaultTopic) {
        Header override = record.headers().lastHeader(H_DST_TOPIC);
        String topic = override == null || override.value() == null
            ? defaultTopic : new String(override.value(), StandardCharsets.UTF_8);
        validateTopic(topic);
        RecordHeaders headers = new RecordHeaders();
        for (Header header : record.headers()) {
            if (!H_DST_TOPIC.equals(header.key())) {
                byte[] value = header.value();
                headers.add(header.key(), value == null ? null : value.clone());
            }
        }
        return new ProducerRecord<>(topic, null, record.timestamp(), record.key(), record.value(), headers);
    }

    static void validateTopic(String topic) {
        if (topic == null || topic.isEmpty() || topic.length() > 249 || topic.equals(".")
                || topic.equals("..") || !topic.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException("Kafka topic must be 1-249 ASCII letters, digits, '.', '_' or '-', excluding '.' and '..'");
        }
    }
}
