package com.tencent.cloud.mqtt;

import java.util.List;
import java.util.Optional;

import org.apache.kafka.streams.processor.api.Record;

public interface Transform<K, V> {
    Optional<List<Record<K, V>>> transform(Record<K, V> record);
}
