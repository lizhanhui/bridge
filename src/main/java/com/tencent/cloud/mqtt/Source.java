package com.tencent.cloud.mqtt;

import org.apache.kafka.streams.processor.api.Record;

public interface Source extends AutoCloseable {
    // Blocks until a record is available; returns null when closed and drained
    Record<String, String> poll() throws InterruptedException;

    @Override
    void close();
}
