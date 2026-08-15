package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

class TaskTest {

    static class FakeSource implements Source {
        final LinkedBlockingQueue<Record<String, String>> records = new LinkedBlockingQueue<>();
        volatile boolean closed;

        @Override
        public Record<String, String> poll() throws InterruptedException {
            // null when closed and drained, like MqttSource
            while (true) {
                Record<String, String> r = records.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
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
        volatile boolean closed;

        @Override
        public void publish(Record<String, String> record) {
            published.add(record);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Test
    void recordsFlowFromSourceThroughTransformToSink() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(new Record<>("home/room/1", "{\"age\":20}", 1L));
        FakeSink sink = new FakeSink();
        // identity transform
        Task task = new Task("t", source, r -> Optional.of(List.of(r)), sink, 1);

        Thread thread = Thread.ofVirtual().start(() -> {
            try {
                task.launch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        // wait until the record reaches the sink, then stop the task
        long deadline = System.currentTimeMillis() + 5000;
        while (sink.published.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        thread.interrupt();
        thread.join();

        assertEquals(1, sink.published.size());
        assertEquals("{\"age\":20}", sink.published.get(0).value());
        assertTrue(source.closed);
        assertTrue(sink.closed);
    }

    @Test
    void filteredRecordsAreNotPublished() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(new Record<>("home/room/1", "{\"age\":99}", 1L));
        FakeSink sink = new FakeSink();
        // transform filters everything out
        Task task = new Task("t", source, r -> Optional.empty(), sink, 1);

        Thread thread = Thread.ofVirtual().start(() -> {
            try {
                task.launch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread.sleep(200); // give the loop time to consume the record
        thread.interrupt();
        thread.join();

        assertTrue(sink.published.isEmpty());
        assertTrue(source.closed);
        assertTrue(sink.closed);
    }

    @Test
    void freshRecordGetsHopCountOne() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(new Record<>("k", "{}", 1L));
        FakeSink sink = new FakeSink();
        runUntilConsumed(new Task("t", source, r -> Optional.of(List.of(r)), sink, 1), sink);

        assertEquals(1, sink.published.size());
        assertEquals("1", hopCountOf(sink.published.get(0)));
    }

    @Test
    void hopCountIsIncremented() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(recordWithHopCount("1"));
        FakeSink sink = new FakeSink();
        runUntilConsumed(new Task("t", source, r -> Optional.of(List.of(r)), sink, 2), sink);

        assertEquals(1, sink.published.size());
        assertEquals("2", hopCountOf(sink.published.get(0)));
    }

    @Test
    void recordExceedingMaxHopsIsSkipped() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(recordWithHopCount("2"));
        FakeSink sink = new FakeSink();
        runUntilConsumed(new Task("t", source, r -> Optional.of(List.of(r)), sink, 1), sink);

        assertTrue(sink.published.isEmpty());
    }

    @Test
    void malformedHopCountIsTreatedAsZero() throws Exception {
        FakeSource source = new FakeSource();
        source.records.add(recordWithHopCount("not-a-number"));
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

    /** Runs the task until the record has been consumed, then stops it. */
    private static void runUntilConsumed(Task task, FakeSink sink) throws InterruptedException {
        Thread thread = Thread.ofVirtual().start(() -> {
            try {
                task.launch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        long deadline = System.currentTimeMillis() + 5000;
        while (sink.published.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        Thread.sleep(100); // grace for skipped records to be dropped
        thread.interrupt();
        thread.join();
    }
}
