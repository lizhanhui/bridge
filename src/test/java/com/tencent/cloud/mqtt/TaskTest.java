package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

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
        final List<Record<String, String>> published = new ArrayList<>();
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
        Task task = new Task("t", source, r -> Optional.of(List.of(r)), sink);

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
        Task task = new Task("t", source, r -> Optional.empty(), sink);

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
}
