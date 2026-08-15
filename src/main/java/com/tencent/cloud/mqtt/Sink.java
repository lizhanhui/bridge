package com.tencent.cloud.mqtt;

import org.apache.kafka.streams.processor.api.Record;

public interface Sink extends AutoCloseable {
    void publish(Record<String, String> record);

    @Override
    void close();
}
