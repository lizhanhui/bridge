package com.tencent.cloud.mqtt;

public interface Source extends AutoCloseable {
    /**
     * Blocks until a record is available; returns null when closed and drained.
     * The returned record must be acked ({@link AckableRecord#ack()}) by the
     * consumer once it is fully processed; an unacked record is redelivered
     * by the broker.
     */
    AckableRecord poll() throws InterruptedException;

    @Override
    void close();
}
