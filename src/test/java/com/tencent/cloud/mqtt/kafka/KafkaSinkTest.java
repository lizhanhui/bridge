package com.tencent.cloud.mqtt.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

import com.tencent.cloud.mqtt.AckableRecord;
import com.tencent.cloud.mqtt.Source;
import com.tencent.cloud.mqtt.Task;
import com.tencent.cloud.mqtt.model.Connector;

class KafkaSinkTest {
    private static final Record<String, String> RECORD = new Record<>("key", "value", 123L);

    private static class TrackingProducer extends MockProducer<String, String> {
        final CountDownLatch waiting = new CountDownLatch(1);
        final CountDownLatch retried = new CountDownLatch(1);
        final CountDownLatch closing = new CountDownLatch(1);
        final AtomicInteger sendCount = new AtomicInteger();
        volatile Duration closeTimeout;

        TrackingProducer(boolean autoComplete) {
            super(autoComplete, null, new StringSerializer(), new StringSerializer());
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
            int attempt = sendCount.incrementAndGet();
            Future<RecordMetadata> pending = super.send(record);
            if (attempt > 1) {
                retried.countDown();
            }
            return new Future<>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return pending.cancel(mayInterruptIfRunning);
                }

                @Override
                public boolean isCancelled() {
                    return pending.isCancelled();
                }

                @Override
                public boolean isDone() {
                    return pending.isDone();
                }

                @Override
                public RecordMetadata get() throws InterruptedException, ExecutionException {
                    waiting.countDown();
                    return pending.get();
                }

                @Override
                public RecordMetadata get(long timeout, TimeUnit unit)
                        throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
                    waiting.countDown();
                    return pending.get(timeout, unit);
                }
            };
        }

        @Override
        public void close(Duration timeout) {
            closeTimeout = timeout;
            try {
                super.close(timeout);
            } finally {
                closing.countDown();
            }
        }
    }

    private static class Worker implements AutoCloseable {
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CountDownLatch finished = new CountDownLatch(1);
        final Thread thread;
        volatile boolean interrupted;

        Worker(ThrowingAction action) {
            thread = Thread.ofVirtual().start(() -> {
                try {
                    action.run();
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    interrupted = Thread.currentThread().isInterrupted();
                    finished.countDown();
                }
            });
        }

        void awaitFinished() throws InterruptedException {
            assertTrue(finished.await(5, TimeUnit.SECONDS), "worker must finish");
            thread.join(5000);
            assertFalse(thread.isAlive());
        }

        @Override
        public void close() throws InterruptedException {
            if (thread.isAlive()) {
                thread.interrupt();
            }
            thread.join(5000);
            assertFalse(thread.isAlive(), "worker must not leak");
        }
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private static InterruptException interruption(String message) {
        // Kafka's constructor interrupts its calling thread; only the worker should be interrupted.
        InterruptException failure = new InterruptException(message);
        Thread.interrupted();
        return failure;
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(5, TimeUnit.SECONDS), "expected producer/task event");
    }

    @Test
    void publishWaitsForProducerCompletionAndForwardsRecord() throws Exception {
        TrackingProducer producer = new TrackingProducer(false);
        KafkaSink sink = new KafkaSink(producer, "out");
        try (Worker worker = new Worker(() -> sink.publish(RECORD))) {
            await(producer.waiting);
            assertEquals(1, producer.history().size());
            assertEquals(1, worker.finished.getCount(), "send enqueue alone is not success");
            assertEquals("out", producer.history().getFirst().topic());
            assertEquals("key", producer.history().getFirst().key());
            assertEquals("value", producer.history().getFirst().value());
            assertEquals(123L, producer.history().getFirst().timestamp());
            assertTrue(producer.completeNext());
            worker.awaitFinished();
            assertNull(worker.failure.get());
        } finally {
            sink.close();
        }
    }

    @Test
    void synchronousRecordSpecificFailuresBecomePoison() {
        for (RuntimeException error : poisonErrors()) {
            TrackingProducer producer = new TrackingProducer(true);
            producer.sendException = error;
            KafkaSink sink = new KafkaSink(producer, "out");
            try {
                IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> sink.publish(RECORD));
                assertSame(error, thrown.getCause());
            } finally {
                sink.close();
            }
        }
    }

    @Test
    void asynchronousRecordSpecificFailuresBecomePoison() throws Exception {
        for (RuntimeException error : poisonErrors()) {
            TrackingProducer producer = new TrackingProducer(false);
            KafkaSink sink = new KafkaSink(producer, "out");
            try (Worker worker = new Worker(() -> sink.publish(RECORD))) {
                await(producer.waiting);
                assertTrue(producer.errorNext(error));
                worker.awaitFinished();
                IllegalArgumentException thrown = assertInstanceOf(IllegalArgumentException.class, worker.failure.get());
                assertSame(error, thrown.getCause());
            } finally {
                sink.close();
            }
        }
    }

    private static List<RuntimeException> poisonErrors() {
        return List.of(new RecordTooLargeException("too large"), new InvalidTopicException("bad topic"),
            new InvalidTimestampException("bad timestamp"), new SerializationException("bad serialization"));
    }

    private static List<RuntimeException> retryErrors() {
        return List.of(new TimeoutException("timeout"), new NetworkException("network"),
            new AuthenticationException("authentication"), new TopicAuthorizationException("authorization"));
    }

    @Test
    void synchronousConnectivityAndSecurityFailuresStayRetryable() {
        for (RuntimeException error : retryErrors()) {
            TrackingProducer producer = new TrackingProducer(true);
            producer.sendException = error;
            KafkaSink sink = new KafkaSink(producer, "out");
            try {
                assertSame(error, assertThrows(RuntimeException.class, () -> sink.publish(RECORD)));
            } finally {
                sink.close();
            }
        }
    }

    @Test
    void asynchronousConnectivityAndSecurityFailuresStayRetryable() throws Exception {
        for (RuntimeException error : retryErrors()) {
            TrackingProducer producer = new TrackingProducer(false);
            KafkaSink sink = new KafkaSink(producer, "out");
            try (Worker worker = new Worker(() -> sink.publish(RECORD))) {
                await(producer.waiting);
                assertTrue(producer.errorNext(error));
                worker.awaitFinished();
                assertSame(error, worker.failure.get());
            } finally {
                sink.close();
            }
        }
    }

    @Test
    void interruptedFutureWaitRestoresInterruptStatus() throws Exception {
        TrackingProducer producer = new TrackingProducer(false);
        KafkaSink sink = new KafkaSink(producer, "out");
        try (Worker worker = new Worker(() -> sink.publish(RECORD))) {
            await(producer.waiting);
            worker.thread.interrupt();
            worker.awaitFinished();
            assertInstanceOf(InterruptException.class, worker.failure.get());
            assertTrue(worker.interrupted);
        } finally {
            sink.close();
        }
    }

    @Test
    void sendInterruptionRestoresInterruptStatusEvenWhenProducerClearsIt() throws Exception {
        TrackingProducer producer = new TrackingProducer(true);
        producer.sendException = interruption("send interrupted");
        KafkaSink sink = new KafkaSink(producer, "out");
        try (Worker worker = new Worker(() -> sink.publish(RECORD))) {
            worker.awaitFinished();
            assertSame(producer.sendException, worker.failure.get());
            assertTrue(worker.interrupted);
        } finally {
            sink.close();
        }
    }

    @Test
    void asynchronousKafkaInterruptionAlsoRestoresInterruptStatus() throws Exception {
        TrackingProducer producer = new TrackingProducer(false);
        KafkaSink sink = new KafkaSink(producer, "out");
        InterruptException interruption = interruption("async interrupted");
        try (Worker worker = new Worker(() -> sink.publish(RECORD))) {
            await(producer.waiting);
            assertTrue(producer.errorNext(interruption));
            worker.awaitFinished();
            assertSame(interruption, worker.failure.get());
            assertTrue(worker.interrupted);
        } finally {
            sink.close();
        }
    }

    @Test
    void invalidConfiguredTopicFailsBeforeProducerConstruction() {
        for (String topic : new String[] {null, "", ".", "..", "bad/topic", "x".repeat(250)}) {
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> new KafkaSink(new Connector(), topic, "sink-task-0"));
            assertTrue(failure.getMessage().toLowerCase().contains("topic"));
        }
    }

    @Test
    void invalidRoutingOverrideNeverReachesProducer() {
        TrackingProducer producer = new TrackingProducer(true);
        KafkaSink sink = new KafkaSink(producer, "out");
        Record<String, String> record = new Record<>("key", "value", 1L);
        record.headers().add("dst.kafka.topic", new byte[0]);
        try {
            assertThrows(IllegalArgumentException.class, () -> sink.publish(record));
            assertEquals(0, producer.sendCount.get());
        } finally {
            sink.close();
        }
    }

    @Test
    void closeUsesBoundedTenSecondTimeout() {
        TrackingProducer producer = new TrackingProducer(true);
        new KafkaSink(producer, "out").close();
        assertEquals(Duration.ofSeconds(10), producer.closeTimeout);
        assertTrue(producer.closed());
    }

    @Test
    void closeRestoresPreexistingInterruptEvenIfProducerClearsItWithoutThrowing() {
        TrackingProducer producer = new TrackingProducer(true) {
            @Override
            public void close(Duration timeout) {
                Thread.interrupted();
                super.close(timeout);
            }
        };
        try {
            Thread.currentThread().interrupt();
            new KafkaSink(producer, "out").close();
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(Duration.ofSeconds(10), producer.closeTimeout);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void closeInterruptionIsBestEffortAndPreservesInterruptState() {
        TrackingProducer producer = new TrackingProducer(true);
        producer.closeException = interruption("close interrupted");
        try {
            assertDoesNotThrow(() -> new KafkaSink(producer, "out").close());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(Duration.ofSeconds(10), producer.closeTimeout);
        } finally {
            Thread.interrupted();
        }
    }

    private static class TestRecord extends AckableRecord {
        final AtomicInteger ackCount = new AtomicInteger();
        final CountDownLatch acked = new CountDownLatch(1);

        TestRecord() {
            super(RECORD);
        }

        @Override
        protected void doAck() {
            ackCount.incrementAndGet();
            acked.countDown();
        }

        @Override
        protected String debugString() {
            return "Kafka sink test record";
        }
    }

    private static class OneRecordSource implements Source {
        private TestRecord record;
        volatile boolean closed;

        OneRecordSource(TestRecord record) {
            this.record = record;
        }

        @Override
        public AckableRecord poll() {
            TestRecord next = record;
            record = null;
            return next;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static Task task(OneRecordSource source, KafkaSink sink) {
        return new Task("kafka-test-0", source, record -> Optional.of(List.of(record)), sink, 1);
    }

    @Test
    void taskAcknowledgesOnlyAfterBrokerSuccess() throws Exception {
        TrackingProducer producer = new TrackingProducer(false);
        KafkaSink sink = new KafkaSink(producer, "out");
        TestRecord record = new TestRecord();
        OneRecordSource source = new OneRecordSource(record);
        try (Worker worker = new Worker(() -> task(source, sink).launch())) {
            await(producer.waiting);
            assertEquals(0, record.ackCount.get());
            assertEquals(1, worker.finished.getCount());
            assertTrue(producer.completeNext());
            await(record.acked);
            worker.awaitFinished();
            assertNull(worker.failure.get());
            assertEquals(1, record.ackCount.get());
            assertTrue(source.closed);
            assertTrue(producer.closed());
        }
    }

    @Test
    void taskInterruptionWhilePublicationPendingRemainsUnacknowledgedAndCloseCannotMaskIt() throws Exception {
        TrackingProducer producer = new TrackingProducer(false);
        producer.closeException = interruption("close interrupted");
        KafkaSink sink = new KafkaSink(producer, "out");
        TestRecord record = new TestRecord();
        OneRecordSource source = new OneRecordSource(record);
        try (Worker worker = new Worker(() -> task(source, sink).launch())) {
            await(producer.waiting);
            worker.thread.interrupt();
            worker.awaitFinished();
            assertInstanceOf(InterruptedException.class, worker.failure.get());
            assertEquals(0, record.ackCount.get());
            assertTrue(source.closed);
            assertTrue(worker.interrupted, "interrupted close must preserve status");
            assertEquals(Duration.ofSeconds(10), producer.closeTimeout);
        }
    }

    @Test
    void taskConnectivityAndSecurityFailuresDoNotAcknowledgeBeforeShutdown() throws Exception {
        for (RuntimeException error : retryErrors()) {
            TrackingProducer producer = new TrackingProducer(false);
            KafkaSink sink = new KafkaSink(producer, "out");
            TestRecord record = new TestRecord();
            OneRecordSource source = new OneRecordSource(record);
            try (Worker worker = new Worker(() -> task(source, sink).launch())) {
                await(producer.waiting);
                assertTrue(producer.errorNext(error));
                await(producer.retried);
                assertEquals(2, producer.sendCount.get(), "failure must reach Task's retry path");
                assertEquals(0, record.ackCount.get());
                worker.thread.interrupt();
                worker.awaitFinished();
                assertInstanceOf(InterruptedException.class, worker.failure.get());
                assertEquals(0, record.ackCount.get());
                assertTrue(source.closed);
                assertTrue(producer.closed());
            }
        }
    }

    @Test
    void taskPoisonUsesExistingSkipAndAcknowledgePolicy() throws Exception {
        TrackingProducer producer = new TrackingProducer(false);
        KafkaSink sink = new KafkaSink(producer, "out");
        TestRecord record = new TestRecord();
        OneRecordSource source = new OneRecordSource(record);
        try (Worker worker = new Worker(() -> task(source, sink).launch())) {
            await(producer.waiting);
            assertTrue(producer.errorNext(new RecordTooLargeException("too large")));
            await(record.acked);
            worker.awaitFinished();
            assertNull(worker.failure.get());
            assertEquals(1, record.ackCount.get());
            assertEquals(1, producer.sendCount.get());
            assertTrue(source.closed);
            assertTrue(producer.closed());
        }
    }
}
