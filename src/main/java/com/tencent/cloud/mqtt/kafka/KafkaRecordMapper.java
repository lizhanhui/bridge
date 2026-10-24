package com.tencent.cloud.mqtt.kafka;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;

public final class KafkaRecordMapper {
    public static final String H_DST_TOPIC = "dst.kafka.topic";

    private KafkaRecordMapper() {}

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
