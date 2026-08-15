package com.tencent.cloud.mqtt;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
    private static final Logger log = LoggerFactory.getLogger(Task.class);

    /**
     * Loop-prevention header: incremented on every pass through this bridge.
     * Travels as an MQTT user property / RocketMQ user property via the record
     * headers, so a message looping back to a source arrives with the count
     * from its previous hops.
     */
    public static final String HOP_COUNT_HEADER = "bridge-hop-count";

    private final String name;

    private final Source source;

    private final Transform<String, String> transform;

    private final Sink sink;

    /** Records arriving with a hop count greater than this are skipped (and thereby acked). */
    private final int maxHops;

    public Task(String name, Source source, Transform<String, String> transform, Sink sink, int maxHops) {
        this.name = name;
        this.source = source;
        this.transform = transform;
        this.sink = sink;
        this.maxHops = maxHops;
    }

    public String getName() {
        return name;
    }

    /**
     * Crash-safety contract: a polled record is acked at the source only once
     * it is fully processed — filtered out by the transform, dropped by the
     * max_hops check, or after every generated result record has been
     * published to the sink ({@link Sink#publish} is blocking and fail-fast,
     * so reaching the ack means the sink durably processed all results).
     * A record whose processing dies mid-flight stays unacked and is
     * redelivered by the broker after restart.
     */
    public void launch() throws InterruptedException {
        log.info("Task {} started", name);
        try {
            AckableRecord record;
            while ((record = source.poll()) != null) {
                int hopCount = hopCount(record);
                if (hopCount > maxHops) {
                    log.info("Task {} dropping record: {}={} exceeds max_hops {}", name,
                        HOP_COUNT_HEADER, hopCount, maxHops);
                    record.ack();
                    continue;
                }
                setHopCount(record, hopCount + 1);
                transform.transform(record)
                    .ifPresent(records -> records.forEach(sink::publish));
                record.ack();
            }
        } finally {
            try {
                source.close();
            } finally {
                sink.close();
            }
            log.info("Task {} stopped", name);
        }
    }

    private static int hopCount(Record<String, String> record) {
        Header header = record.headers().lastHeader(HOP_COUNT_HEADER);
        if (header == null || header.value() == null) {
            return 0;
        }
        String value = new String(header.value(), StandardCharsets.UTF_8);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("Malformed {} header '{}', treating as 0", HOP_COUNT_HEADER, value);
            return 0;
        }
    }

    private static void setHopCount(Record<String, String> record, int hopCount) {
        record.headers().remove(HOP_COUNT_HEADER);
        record.headers().add(HOP_COUNT_HEADER,
            Integer.toString(hopCount).getBytes(StandardCharsets.UTF_8));
    }
}
