package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        private final List<String> events;
        volatile boolean closed;
        volatile boolean failOnPublish;

        FakeSink() {
            this(null);
        }

        FakeSink(List<String> events) {
            this.events = events;
        }

        @Override
        public void publish(Record<String, String> record) {
            if (failOnPublish) {
                throw new RuntimeException("publish failed");
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
    void recordIsNotAckedWhenSinkThrows() throws Exception {
        FakeSource source = new FakeSource();
        TestAckableRecord record = new TestAckableRecord(new Record<>("k", "{}", 1L));
        source.records.add(record);
        FakeSink sink = new FakeSink();
        sink.failOnPublish = true;
        Task task = new Task("t", source, r -> Optional.of(List.of(r)), sink, 1);

        Thread thread = startTask(task);
        thread.join(); // task dies on the publish failure

        assertTrue(sink.published.isEmpty());
        assertEquals(0, record.ackCount.get());
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
