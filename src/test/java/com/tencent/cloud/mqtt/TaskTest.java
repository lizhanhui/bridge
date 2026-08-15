package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

class TaskTest {

    /** AckableRecord with a counting, optionally event-logging ack. */
    static class TestAckableRecord extends AckableRecord {
        final AtomicInteger ackCount = new AtomicInteger();
        private final List<String> events;

        TestAckableRecord(Record<String, String> base) {
            this(base, null);
        }

        TestAckableRecord(Record<String, String> base, List<String> events) {
            super(base);
            this.events = events;
        }

        @Override
        protected void doAck() {
            ackCount.incrementAndGet();
            if (events != null) {
                events.add("ack");
            }
        }
    }

    static class FakeSource implements Source {
        final LinkedBlockingQueue<AckableRecord> records = new LinkedBlockingQueue<>();
        volatile boolean closed;

        @Override
        public AckableRecord poll() throws InterruptedException {
            // null when closed and drained, like MqttSource
            while (true) {
                AckableRecord r = records.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (r != null) return r;
                if (closed && records.isEmpty()) return null;
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    static class FakeSink implements Sink {
        final List<Record<String, String>> published = new CopyOnWriteArrayList<>();
        final AtomicInteger publishAttempts = new AtomicInteger();
        final AtomicInteger recoverableFailures = new AtomicInteger();
        private final List<String> events;
        volatile boolean closed;
        volatile RuntimeException failWith;

        FakeSink() {
            this(null);
        }

        FakeSink(List<String> events) {
            this.events = events;
        }

        @Override
        public void publish(Record<String, String> record) {
            publishAttempts.incrementAndGet();
            if (failWith != null) {
                throw failWith;
            }
            if (recoverableFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new RuntimeException("transient publish failure");
            }
            published.add(record);
            if (events != null) {
                events.add("publish");
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Test
    void recordsFlowFromSourceThroughTransformToSink() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(new TestAckableRecord(new Record<>("home/room/1", "{\"age\":20}", 1L)));
        FakeSink sink = new FakeSink();
        // identity transform
        Task task = new Task("t", source, r -> Optional.of(List.of(r)), sink, 1);

        Thread thread = startTask(task);
        // wait until the record reaches the sink, then stop the task
        waitFor(() -> !sink.published.isEmpty());
        thread.interrupt();
        thread.join();

        assertEquals(1, sink.published.size());
        assertEquals("{\"age\":20}", sink.published.get(0).value());
        assertTrue(source.closed);
        assertTrue(sink.closed);
    }

    @Test
    void filteredRecordsAreNotPublishedButAcked() throws Exception {
        FakeSource source = new FakeSource();
        TestAckableRecord record = new TestAckableRecord(new Record<>("home/room/1", "{\"age\":99}", 1L));
        source.records.add(record);
        FakeSink sink = new FakeSink();
        // transform filters everything out
        Task task = new Task("t", source, r -> Optional.empty(), sink, 1);

        Thread thread = startTask(task);
        waitFor(() -> record.ackCount.get() == 1);
        thread.interrupt();
        thread.join();

        assertTrue(sink.published.isEmpty());
        assertEquals(1, record.ackCount.get());
        assertTrue(source.closed);
        assertTrue(sink.closed);
    }

    @Test
    void recordIsAckedAfterAllResultRecordsArePublished() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        FakeSource source = new FakeSource();
        TestAckableRecord record = new TestAckableRecord(new Record<>("k", "{}", 1L), events);
        source.records.add(record);
        FakeSink sink = new FakeSink(events);
        // transform emits two result records
        Task task = new Task("t", source,
            r -> Optional.of(List.of(
                new Record<>("k", "a", 1L),
                new Record<>("k", "b", 1L))),
            sink, 1);

        Thread thread = startTask(task);
        waitFor(() -> record.ackCount.get() == 1);
        thread.interrupt();
        thread.join();

        assertEquals(2, sink.published.size());
        assertEquals(List.of("publish", "publish", "ack"), events);
    }

    @Test
    void recoverableSinkFailureIsRetriedUntilPublishSucceeds() throws Exception {
        FakeSource source = new FakeSource();
        TestAckableRecord record = new TestAckableRecord(new Record<>("k", "{}", 1L));
        source.records.add(record);
        FakeSink sink = new FakeSink();
        sink.recoverableFailures.set(2);
        Task task = new Task("t", source, r -> Optional.of(List.of(r)), sink, 1);

        Thread thread = startTask(task);
        waitFor(() -> record.ackCount.get() == 1);
        thread.interrupt();
        thread.join();

        assertEquals(1, sink.published.size());
        assertEquals(3, sink.publishAttempts.get());
        assertEquals(1, record.ackCount.get());
    }

    @Test
    void interruptDuringRetryBackoffStopsTaskWithoutAcking() throws Exception {
        FakeSource source = new FakeSource();
        TestAckableRecord record = new TestAckableRecord(new Record<>("k", "{}", 1L));
        source.records.add(record);
        FakeSink sink = new FakeSink();
        sink.recoverableFailures.set(Integer.MAX_VALUE); // never recovers
        Task task = new Task("t", source, r -> Optional.of(List.of(r)), sink, 1);

        Thread thread = startTask(task);
        waitFor(() -> sink.publishAttempts.get() >= 1); // now in the 1s backoff sleep
        thread.interrupt();
        thread.join(5000);

        assertFalse(thread.isAlive(), "interrupt during backoff should stop the task");
        assertEquals(0, record.ackCount.get());
        assertTrue(source.closed);
        assertTrue(sink.closed);
    }

    @Test
    void poisonFailureSkipsRemainingRowsButAcksSource() throws Exception {
        FakeSource source = new FakeSource();
        TestAckableRecord record = new TestAckableRecord(new Record<>("k", "{}", 1L));
        source.records.add(record);
        FakeSink sink = new FakeSink() {
            @Override
            public void publish(Record<String, String> r) {
                publishAttempts.incrementAndGet();
                if ("poison".equals(r.value())) {
                    throw new IllegalArgumentException("bad row");
                }
                published.add(r);
            }
        };
        Task task = new Task("t", source,
            r -> Optional.of(List.of(
                new Record<>("k", "a", 1L),
                new Record<>("k", "poison", 1L),
                new Record<>("k", "b", 1L))),
            sink, 1);

        Thread thread = startTask(task);
        waitFor(() -> record.ackCount.get() == 1);
        thread.interrupt();
        thread.join();

        // row 1 published; row 2 poisoned; row 3 skipped (break, not continue)
        assertEquals(1, sink.published.size());
        assertEquals("a", sink.published.get(0).value());
        assertEquals(2, sink.publishAttempts.get());
        assertEquals(1, record.ackCount.get());
    }

    @Test
    void poisonSinkFailureIsSkippedAndAcked() throws Exception {
        FakeSource source = new FakeSource();
        TestAckableRecord record = new TestAckableRecord(new Record<>("k", "{}", 1L));
        source.records.add(record);
        FakeSink sink = new FakeSink();
        sink.failWith = new IllegalArgumentException("invalid destination");
        Task task = new Task("t", source, r -> Optional.of(List.of(r)), sink, 1);

        Thread thread = startTask(task);
        waitFor(() -> record.ackCount.get() == 1);
        thread.interrupt();
        thread.join();

        assertTrue(sink.published.isEmpty());
        assertEquals(1, sink.publishAttempts.get());
        assertEquals(1, record.ackCount.get());
        assertTrue(source.closed);
        assertTrue(sink.closed);
    }

    @Test
    void retryBackoffDoublesUntilSixtySeconds() {
        assertEquals(1_000, Task.retryDelayMillis(1));
        assertEquals(2_000, Task.retryDelayMillis(2));
        assertEquals(4_000, Task.retryDelayMillis(3));
        assertEquals(8_000, Task.retryDelayMillis(4));
        assertEquals(16_000, Task.retryDelayMillis(5));
        assertEquals(32_000, Task.retryDelayMillis(6));
        assertEquals(60_000, Task.retryDelayMillis(7));
        assertEquals(60_000, Task.retryDelayMillis(100));
    }

    @Test
    void onlyInvalidArgumentFailuresArePoison() {
        assertTrue(Task.isPoisonSinkFailure(new IllegalArgumentException("bad record")));
        assertFalse(Task.isPoisonSinkFailure(new RuntimeException("broker unavailable")));
    }

    @Test
    void ackIsIdempotent() {
        TestAckableRecord record = new TestAckableRecord(new Record<>("k", "{}", 1L));
        record.ack();
        record.ack();
        assertEquals(1, record.ackCount.get());
    }

    @Test
    void freshRecordGetsHopCountOne() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(new TestAckableRecord(new Record<>("k", "{}", 1L)));
        FakeSink sink = new FakeSink();
        runUntilConsumed(new Task("t", source, r -> Optional.of(List.of(r)), sink, 1), sink);

        assertEquals(1, sink.published.size());
        assertEquals("1", hopCountOf(sink.published.get(0)));
    }

    @Test
    void hopCountIsIncremented() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(new TestAckableRecord(recordWithHopCount("1")));
        FakeSink sink = new FakeSink();
        runUntilConsumed(new Task("t", source, r -> Optional.of(List.of(r)), sink, 2), sink);

        assertEquals(1, sink.published.size());
        assertEquals("2", hopCountOf(sink.published.get(0)));
    }

    @Test
    void recordExceedingMaxHopsIsSkippedAndAcked() throws Exception {
        FakeSource source = new FakeSource();
        TestAckableRecord record = new TestAckableRecord(recordWithHopCount("2"));
        source.records.add(record);
        FakeSink sink = new FakeSink();
        Task task = new Task("t", source, r -> Optional.of(List.of(r)), sink, 1);

        Thread thread = startTask(task);
        waitFor(() -> record.ackCount.get() == 1);
        thread.interrupt();
        thread.join();

        assertTrue(sink.published.isEmpty());
        assertEquals(1, record.ackCount.get());
    }

    @Test
    void malformedHopCountIsTreatedAsZero() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(new TestAckableRecord(recordWithHopCount("not-a-number")));
        FakeSink sink = new FakeSink();
        runUntilConsumed(new Task("t", source, r -> Optional.of(List.of(r)), sink, 1), sink);

        assertEquals(1, sink.published.size());
        assertEquals("1", hopCountOf(sink.published.get(0)));
    }

    private static Record<String, String> recordWithHopCount(String hopCount) {
        Record<String, String> record = new Record<>("k", "{}", 1L);
        record.headers().add("bridge-hop-count", hopCount.getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private static String hopCountOf(Record<String, String> record) {
        Header header = record.headers().lastHeader("bridge-hop-count");
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Starts the task on a virtual thread, swallowing its termination (interrupt or error). */
    private static Thread startTask(Task task) {
        return Thread.ofVirtual().start(() -> {
            try {
                task.launch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable ignored) {
                // tasks are fail-fast; tests assert the aftermath
            }
        });
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    /** Runs the task until the record has been consumed, then stops it. */
    private static void runUntilConsumed(Task task, FakeSink sink) throws InterruptedException {
        Thread thread = startTask(task);
        waitFor(() -> !sink.published.isEmpty());
        Thread.sleep(100); // grace for skipped records to be dropped
        thread.interrupt();
        thread.join();
    }
}
