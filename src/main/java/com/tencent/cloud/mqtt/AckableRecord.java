package com.tencent.cloud.mqtt;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.streams.processor.api.Record;

/**
 * A source record carrying its own ack context. The consumer (see
 * {@link Task}) must call {@link #ack()} once the record is fully processed:
 * filtered out by the transform, deliberately dropped (e.g. max_hops), or
 * after all derived records have been durably sunk. Never acking means the
 * broker redelivers — that is the crash-safety contract.
 *
 * <p>Each {@link Source} implementation provides a subtype holding its own
 * ack context (e.g. the MQTT subtype holds the received {@code Mqtt5Publish}).
 *
 * <p>Caveat: the inherited {@code withKey}/{@code withValue}/
 * {@code withTimestamp}/{@code withHeaders} return plain {@code Record}s and
 * silently drop the ack capability. The bridge mutates headers in place and
 * never uses them; future callers must not either.
 */
public abstract class AckableRecord extends Record<String, String> {

    private final AtomicBoolean acked = new AtomicBoolean();

    /** Shares the base record's {@code Headers} instance. */
    protected AckableRecord(Record<String, String> base) {
        super(base.key(), base.value(), base.timestamp(), base.headers());
    }

    /** Idempotent: only the first call delegates to {@link #doAck()}. */
    public final void ack() {
        if (acked.compareAndSet(false, true)) {
            doAck();
        }
    }

    protected abstract void doAck();
}
