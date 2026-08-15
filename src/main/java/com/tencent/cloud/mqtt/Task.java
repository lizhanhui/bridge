package com.tencent.cloud.mqtt;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
    private static final Logger log = LoggerFactory.getLogger(Task.class);

    /** First sink-retry delay after a recoverable publish failure. */
    static final long INITIAL_RETRY_DELAY_MILLIS = 1_000;

    /** Maximum sink-retry delay; retries continue indefinitely at this interval. */
    static final long MAX_RETRY_DELAY_MILLIS = 60_000;

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
     * max_hops check, skipped as poison by the sink, or after every generated
     * result record has been published to the sink. Recoverable sink failures
     * are retried with backoff before acknowledgement (see
     * {@link #publishWithRetry}); a record still retrying when the process
     * dies stays unacked and is redelivered by the broker after restart.
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
                Optional<List<Record<String, String>>> transformed = transform.transform(record);
                if (transformed.isPresent()) {
                    for (Record<String, String> result : transformed.get()) {
                        if (!publishWithRetry(result)) {
                            break;
                        }
                    }
                }
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

    /**
     * Publishes one transformed record. Recoverable sink failures are retried
     * indefinitely with exponential backoff. A poison failure cannot succeed by
     * retrying, so it and the remaining rows from the same source record are
     * skipped; returning false lets the caller acknowledge the source record.
     */
    private boolean publishWithRetry(Record<String, String> record) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                sink.publish(record);
                return true;
            } catch (RuntimeException e) {
                if (isPoisonSinkFailure(e)) {
                    log.warn("Task {} skipping record after non-retryable sink failure", name, e);
                    return false;
                }
                long delay = retryDelayMillis(attempt);
                log.warn("Task {} sink publish failed; retrying in {} ms", name, delay, e);
                Thread.sleep(delay);
            }
        }
    }

    /** Conservative poison classification: argument validation failures cannot be fixed by retrying. */
    static boolean isPoisonSinkFailure(RuntimeException failure) {
        return failure instanceof IllegalArgumentException;
    }

    /** Delay before sink retry number {@code retry}, doubling from 1s and capped at 60s. */
    static long retryDelayMillis(int retry) {
        int shift = Math.min(Math.max(retry - 1, 0), 31);
        long delay = INITIAL_RETRY_DELAY_MILLIS << shift;
        return Math.min(delay, MAX_RETRY_DELAY_MILLIS);
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
