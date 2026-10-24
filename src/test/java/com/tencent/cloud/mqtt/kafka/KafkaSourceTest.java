package com.tencent.cloud.mqtt.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.tencent.cloud.mqtt.AckableRecord;
import com.tencent.cloud.mqtt.Sink;
import com.tencent.cloud.mqtt.Task;
import com.tencent.cloud.mqtt.model.Connector;

@Timeout(5)
class KafkaSourceTest {
    private static final TopicPartition P0 = new TopicPartition("input", 0);
    private static final TopicPartition P1 = new TopicPartition("input", 1);

    private static Connector connector() {
        Connector connector = new Connector();
        connector.setProperties(Map.of("bootstrap.servers", "localhost:9092", "group.id", "source-test",
            "auto.offset.reset", "earliest"));
        return connector;
    }

    private static ConsumerRecord<String, String> row(TopicPartition partition, long offset, String value) {
        return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, "key-" + offset, value);
    }

    private static KafkaSource source(TrackingConsumer consumer, long interval) {
        return new KafkaSource(connector(), List.of("input"), "source-test", interval, config -> {
            consumer.createdBy = Thread.currentThread();
            assertEquals(false, config.get("enable.auto.commit"));
            assertEquals("read_committed", config.get("isolation.level"));
            assertFalse(config.containsKey("max.poll.records"));
            return consumer;
        });
    }

    private static void batch(TrackingConsumer consumer, ConsumerRecord<String, String>... records) {
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(P0, P1));
            consumer.updateBeginningOffsets(Map.of(P0, 0L, P1, 0L));
            for (var record : records) consumer.addRecord(record);
        });
    }

    private static AckableRecord requireRecord(KafkaSource source) throws InterruptedException {
        AckableRecord record = source.poll();
        assertNotNull(record, "source must deliver buffered record");
        return record;
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(5);
        assertTrue(condition.getAsBoolean(), "condition not reached");
    }

    @Test
    void buffersWholeBatchAndCommitsOnlyCompletedPrefixAcrossOffsetGaps() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 2, "a"), row(P0, 8, "b"), row(P1, 4, "c"));
        try (KafkaSource source = source(consumer, 30)) {
            AckableRecord first = requireRecord(source);
            assertEquals("a", first.value());
            assertEquals("input:0:2", ((KafkaAckableRecord) first).debugString());
            await(() -> consumer.polls.get() >= 3);
            assertTrue(consumer.commits.isEmpty());
            assertTrue(consumer.paused().containsAll(Set.of(P0, P1)));
            first.ack(); first.ack();
            AckableRecord second = requireRecord(source);
            assertEquals("b", second.value());
            await(() -> consumer.hasCommit(P0, 3));
            assertFalse(consumer.hasCommit(P0, 9));
            second.ack();
            AckableRecord third = requireRecord(source);
            assertEquals("c", third.value());
            await(() -> consumer.hasCommit(P0, 9));
            assertFalse(consumer.hasCommit(P1, 5));
            third.ack();
            await(() -> consumer.hasCommit(P1, 5));
        }
        assertTrue(consumer.closed());
        assertTrue(consumer.calls.stream().allMatch(thread -> thread == consumer.createdBy));
    }

    @Test
    void secondPollWaitsForActiveRecordAndInterruptsCleanly() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "first"), row(P0, 1, "second"));
        try (KafkaSource source = source(consumer, 1000)) {
            AckableRecord first = requireRecord(source);
            AtomicReference<Throwable> result = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);
            Thread waiter = Thread.ofVirtual().start(() -> {
                started.countDown();
                try { result.set(new AssertionError("unexpected record " + source.poll())); }
                catch (Throwable failure) { result.set(failure); }
            });
            try {
                assertTrue(started.await(1, TimeUnit.SECONDS));
                Thread.sleep(30);
                assertNull(result.get());
                waiter.interrupt(); waiter.join(1000);
                assertInstanceOf(InterruptedException.class, result.get());
            } finally { waiter.interrupt(); waiter.join(1000); }
            first.ack();
            assertEquals("second", requireRecord(source).value());
        }
        assertTrue(consumer.commits.stream().noneMatch(map -> map.containsKey(P0) && map.get(P0).offset() > 1));
    }

    @Test
    void ackAndNextPollDoNotWaitForBlockedCommitAndLaterCompletionStaysDirty() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        consumer.blockCommit = true;
        batch(consumer, row(P0, 0, "first"), row(P0, 1, "second"));
        try (KafkaSource source = source(consumer, 1)) {
            requireRecord(source).ack();
            assertTrue(consumer.commitEntered.await(1, TimeUnit.SECONDS));
            AckableRecord second = requireRecord(source);
            long start = System.nanoTime();
            second.ack();
            assertTrue(System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(100));
            consumer.releaseCommit.countDown();
            await(() -> consumer.hasCommit(P0, 2));
            assertEquals(1, consumer.commits.getFirst().get(P0).offset());
        } finally { consumer.releaseCommit.countDown(); }
    }

    @Test
    void revokeDropsQueuedRowsAndFencesOldActiveAfterSamePartitionReassignment() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "old-active"), row(P0, 1, "old-queued"));
        try (KafkaSource source = source(consumer, 1)) {
            AckableRecord old = requireRecord(source);
            CountDownLatch assigned = new CountDownLatch(1);
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(P1));
                consumer.rebalance(List.of(P0, P1));
                consumer.addRecord(row(P0, 0, "new"));
                assigned.countDown();
            });
            assertTrue(assigned.await(1, TimeUnit.SECONDS));
            assertTrue(consumer.paused().contains(P0));
            old.ack();
            AckableRecord fresh = requireRecord(source);
            assertEquals("new", fresh.value());
            assertTrue(consumer.commits.isEmpty());
            fresh.ack();
            await(() -> consumer.hasCommit(P0, 1));
        }
    }

    @Test
    void cooperativeRebalanceRetainsOtherPartitionQueueAndLease() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "active"), row(P0, 1, "retained"), row(P1, 0, "revoked"));
        try (KafkaSource source = source(consumer, 1)) {
            AckableRecord first = requireRecord(source);
            CountDownLatch rebalanced = new CountDownLatch(1);
            consumer.schedulePollTask(() -> { consumer.rebalance(List.of(P0)); rebalanced.countDown(); });
            assertTrue(rebalanced.await(1, TimeUnit.SECONDS));
            first.ack();
            AckableRecord retained = requireRecord(source);
            assertEquals("retained", retained.value());
            retained.ack();
            await(() -> consumer.hasCommit(P0, 2));
            assertTrue(consumer.commits.stream().noneMatch(map -> map.containsKey(P1)));
        }
    }

    @Test
    void lostPartitionsNeverCommitTheirCompletedOffsets() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "completed"), row(P0, 1, "active"));
        try (KafkaSource source = source(consumer, 10000)) {
            requireRecord(source).ack();
            AckableRecord active = requireRecord(source);
            CountDownLatch lost = new CountDownLatch(1);
            consumer.schedulePollTask(() -> { consumer.losePartitions(List.of(P0)); lost.countDown(); });
            assertTrue(lost.await(1, TimeUnit.SECONDS));
            active.ack();
        }
        assertTrue(consumer.commits.isEmpty());
    }

    @Test
    void revokeCommitTimeoutDoesNotKillHealthyRebalance() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "done"), row(P0, 1, "old"));
        try (KafkaSource source = source(consumer, 10000)) {
            requireRecord(source).ack();
            AckableRecord old = requireRecord(source);
            consumer.commitFailure = new TimeoutException("revoke timeout");
            CountDownLatch rebound = new CountDownLatch(1);
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(P1)); consumer.rebalance(List.of(P0, P1));
                consumer.addRecord(row(P0, 0, "new")); rebound.countDown();
            });
            assertTrue(rebound.await(1, TimeUnit.SECONDS));
            old.ack();
            assertEquals("new", requireRecord(source).value());
        }
        assertEquals(1, consumer.attempts.get());
    }

    @Test
    void retriableCommitReturnsToMembershipPollingAndRetries() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        consumer.commitFailure = new TimeoutException("retry");
        batch(consumer, row(P0, 0, "done"), row(P0, 1, "active"));
        try (KafkaSource source = source(consumer, 1)) {
            requireRecord(source).ack();
            source.poll();
            await(() -> consumer.hasCommit(P0, 1));
            assertTrue(consumer.polls.get() >= 2);
            assertTrue(consumer.attempts.get() >= 2);
        }
    }

    @Test
    void fatalOwnerFailureInterruptsTaskDuringIndefiniteSinkRetryAndRemainsObservable() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "{}"));
        AuthenticationException fatal = new AuthenticationException("fatal original");
        KafkaSource source = source(consumer, 1000);
        CountDownLatch sinkCalled = new CountDownLatch(1);
        AtomicReference<Throwable> result = new AtomicReference<>();
        Sink sink = new Sink() {
            public void publish(org.apache.kafka.streams.processor.api.Record<String, String> record) {
                sinkCalled.countDown(); throw new IllegalStateException("sink offline");
            }
            public void close() {}
        };
        Thread lane = Thread.ofVirtual().start(() -> {
            try { new Task("retry", source, record -> java.util.Optional.of(List.of(record)), sink, 1).launch(); }
            catch (Throwable failure) { result.set(failure); }
        });
        try {
            assertTrue(sinkCalled.await(1, TimeUnit.SECONDS));
            consumer.schedulePollTask(() -> { throw fatal; });
            lane.join(2000);
            assertFalse(lane.isAlive());
            assertInstanceOf(InterruptedException.class, result.get());
            assertSame(fatal, assertThrows(AuthenticationException.class, source::poll));
            assertTrue(consumer.commits.isEmpty());
        } finally { lane.interrupt(); source.close(); lane.join(1000); }
    }

    @Test
    void tombstonesAreCompletedInOrderBeforeAndAfterValidRows() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, null), row(P0, 1, "valid"), row(P0, 2, null));
        KafkaSource source = source(consumer, 1);
        Thread waiter = null;
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        try {
            AckableRecord valid = requireRecord(source);
            assertEquals("valid", valid.value());
            await(() -> consumer.hasCommit(P0, 1));
            assertFalse(consumer.hasCommit(P0, 3));
            valid.ack();
            waiter = Thread.ofVirtual().start(() -> {
                try { source.poll(); }
                catch (InterruptedException expected) { Thread.currentThread().interrupt(); }
                catch (Throwable failure) { workerFailure.set(failure); }
            });
            await(() -> consumer.hasCommit(P0, 3));
        } finally {
            source.close();
            if (waiter != null) { waiter.interrupt(); waiter.join(1000); assertFalse(waiter.isAlive()); }
        }
        assertNull(workerFailure.get(), "tombstone worker must not hide an unexpected failure");
    }

    @Test
    void closeFlushesOnlyCompletedWorkAndPreservesAlreadyInterruptedCaller() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "done"), row(P0, 1, "unfinished"), row(P0, 2, "queued"));
        KafkaSource source = source(consumer, 10000);
        requireRecord(source).ack();
        source.poll();
        Thread.currentThread().interrupt();
        try {
            source.close(); source.close();
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(consumer.closed());
            assertTrue(consumer.hasCommit(P0, 1));
            assertFalse(consumer.hasCommit(P0, 2));
        } finally { Thread.interrupted(); }
        assertNull(source.poll());
    }

    @Test
    void startupFailureClosesPartialConsumerAndPropagatesOriginalCause() {
        TrackingConsumer consumer = new TrackingConsumer();
        AuthenticationException original = new AuthenticationException("subscribe failed");
        consumer.subscribeFailure = original;
        assertSame(original, assertThrows(AuthenticationException.class, () -> source(consumer, 1000)));
        assertTrue(consumer.closed());
    }

    @Test
    void validatesTopicsIntervalGroupAndResetBeforeCreatingClient() {
        for (List<String> topics : List.of(List.<String>of(), List.of("bad/topic"), List.of("."))) {
            assertThrows(IllegalArgumentException.class, () -> new KafkaSource(connector(), topics, "id", 1,
                config -> { fail("created invalid client"); return null; }));
        }
        assertThrows(IllegalArgumentException.class, () -> new KafkaSource(connector(), List.of("input"), "id", 0,
            config -> { fail("created invalid client"); return null; }));
        for (String key : List.of("group.id", "auto.offset.reset")) {
            Connector connector = connector();
            var properties = new java.util.HashMap<>(connector.getProperties()); properties.remove(key);
            connector.setProperties(properties);
            assertThrows(IllegalArgumentException.class, () -> new KafkaSource(connector, List.of("input"), "id", 1,
                config -> { fail("created invalid client"); return null; }));
        }
    }

    @Test
    void rebalanceInProgressCommitIsResnapshottedAfterFreshLeaseAssignment() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "done"), row(P0, 1, "old-active"));
        try (KafkaSource source = source(consumer, 1)) {
            CountDownLatch failedCommit = new CountDownLatch(1);
            CountDownLatch activeClaimed = new CountDownLatch(1);
            consumer.commitHook = () -> {
                try { assertTrue(activeClaimed.await(1, TimeUnit.SECONDS)); }
                catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
                consumer.losePartitions(List.of(P0));
                consumer.rebalance(List.of(P0, P1));
                consumer.addRecord(row(P0, 20, "fresh"));
                failedCommit.countDown();
                throw new RebalanceInProgressException("membership changed during commit");
            };
            requireRecord(source).ack();
            AckableRecord old = requireRecord(source);
            activeClaimed.countDown();
            assertTrue(failedCommit.await(1, TimeUnit.SECONDS));
            old.ack();
            AckableRecord fresh = requireRecord(source);
            assertEquals("fresh", fresh.value());
            assertTrue(consumer.commits.isEmpty());
            fresh.ack();
            await(() -> consumer.hasCommit(P0, 21));
            assertFalse(consumer.hasCommit(P0, 1));
        }
    }

    @Test
    void newlyAssignedPartitionsStayPausedUntilOldActiveCompletes() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(P0));
            consumer.updateBeginningOffsets(Map.of(P0, 0L, P1, 0L));
            consumer.addRecord(row(P0, 0, "active"));
        });
        try (KafkaSource source = source(consumer, 1000)) {
            AckableRecord active = requireRecord(source);
            CountDownLatch assigned = new CountDownLatch(1);
            consumer.schedulePollTask(() -> {
                consumer.rebalance(List.of(P0, P1));
                consumer.addRecord(row(P1, 0, "new partition"));
                assigned.countDown();
            });
            assertTrue(assigned.await(1, TimeUnit.SECONDS));
            assertTrue(consumer.paused().containsAll(Set.of(P0, P1)));
            int polls = consumer.polls.get();
            await(() -> consumer.polls.get() >= polls + 2);
            assertTrue(consumer.commits.isEmpty());
            active.ack();
            assertEquals("new partition", requireRecord(source).value());
        }
    }

    @Test
    void fatalBeforeNextTaskPollPrecedesBufferedDeliveryAndAckShowsOriginalCause() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "active"), row(P0, 1, "buffered"));
        KafkaSource source = source(consumer, 1000);
        AtomicReference<AckableRecord> active = new AtomicReference<>();
        Thread lane = Thread.ofVirtual().start(() -> {
            try { active.set(requireRecord(source)); }
            catch (InterruptedException failure) { throw new AssertionError(failure); }
        });
        try {
            lane.join(1000); assertFalse(lane.isAlive());
            IllegalStateException original = new IllegalStateException("owner broke");
            consumer.schedulePollTask(() -> { throw original; });
            await(consumer::closed);
            assertSame(original, assertThrows(IllegalStateException.class, source::poll));
            assertSame(original, assertThrows(IllegalStateException.class, active.get()::ack));
            assertTrue(consumer.commits.isEmpty());
        } finally { source.close(); lane.interrupt(); lane.join(1000); }
    }

    @Test
    void closeIsIdempotentWhileOwnerCommitIsStillRunning() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        consumer.blockCommit = true;
        batch(consumer, row(P0, 0, "done"));
        KafkaSource source = source(consumer, 1);
        List<Thread> closers = new ArrayList<>();
        try {
            requireRecord(source).ack();
            assertTrue(consumer.commitEntered.await(1, TimeUnit.SECONDS));
            closers.add(Thread.ofVirtual().start(source::close));
            await(() -> consumer.wakeups.get() == 1);
            CountDownLatch secondStarted = new CountDownLatch(1);
            closers.add(Thread.ofVirtual().start(() -> { secondStarted.countDown(); source.close(); }));
            assertTrue(secondStarted.await(1, TimeUnit.SECONDS));
            Thread.sleep(30);
            assertEquals(1, consumer.wakeups.get(), "only the transition to stopping should wake the owner");
        } finally {
            consumer.releaseCommit.countDown();
            for (Thread closer : closers) closer.join(1000);
            source.close();
        }
        assertTrue(consumer.closed());
    }

    @Test
    void interruptedConstructorCleansUpCreatedConsumerOnOwnerAndPreservesInterrupt() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch releaseFactory = new CountDownLatch(1);
        AtomicReference<Throwable> result = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>();
        Thread constructor = Thread.ofVirtual().start(() -> {
            try {
                new KafkaSource(connector(), List.of("input"), "interrupted", 1000, config -> {
                    consumer.createdBy = Thread.currentThread(); factoryEntered.countDown();
                    try { assertTrue(releaseFactory.await(2, TimeUnit.SECONDS)); }
                    catch (InterruptedException failure) { throw new AssertionError(failure); }
                    return consumer;
                });
                result.set(new AssertionError("construction should be interrupted"));
            } catch (Throwable failure) { result.set(failure); interrupted.set(Thread.currentThread().isInterrupted()); }
        });
        try {
            assertTrue(factoryEntered.await(1, TimeUnit.SECONDS));
            constructor.interrupt();
            Thread.sleep(20);
            releaseFactory.countDown();
            constructor.join(1000);
            assertFalse(constructor.isAlive());
            assertInstanceOf(InterruptedException.class, result.get().getCause());
            assertEquals(true, interrupted.get());
            assertTrue(consumer.closed());
            assertTrue(consumer.calls.stream().allMatch(thread -> thread == consumer.createdBy));
        } finally { releaseFactory.countDown(); constructor.interrupt(); constructor.join(1000); }
    }

    @Test
    void ownerFactoryReceivesCopiedConnectorPropertiesDespiteConcurrentMutation() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        Connector connector = connector();
        var properties = new java.util.HashMap<>(connector.getProperties());
        var protocols = new ArrayList<>(List.of("TLSv1.3"));
        properties.put("ssl.enabled.protocols", protocols);
        connector.setProperties(properties);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<KafkaSource> source = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread constructor = Thread.ofVirtual().start(() -> {
            try {
                source.set(new KafkaSource(connector, List.of("input"), "copy", 1000, config -> {
                    entered.countDown();
                    try { assertTrue(release.await(2, TimeUnit.SECONDS)); }
                    catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
                    assertEquals("source-test", config.get("group.id"));
                    assertEquals(List.of("TLSv1.3"), config.get("ssl.enabled.protocols"));
                    return consumer;
                }));
            } catch (Throwable error) { failure.set(error); }
        });
        try {
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            properties.put("group.id", "mutated"); protocols.add("TLSv1.2");
            release.countDown(); constructor.join(1000);
            assertFalse(constructor.isAlive()); assertNull(failure.get()); assertNotNull(source.get());
        } finally {
            release.countDown(); constructor.join(1000);
            if (source.get() != null) source.get().close();
        }
    }

    @Test
    void nativeMaxPollRecordsLimitsTheSingleBufferedBatchWithoutForcingOne() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "0"), row(P0, 1, "1"), row(P0, 2, "2"), row(P0, 3, "3"));
        Connector connector = connector();
        var properties = new java.util.HashMap<>(connector.getProperties()); properties.put("max.poll.records", 2);
        connector.setProperties(properties);
        try (KafkaSource source = new KafkaSource(connector, List.of("input"), "batch-size", 1000, config -> {
            assertEquals(2, config.get("max.poll.records")); consumer.setMaxPollRecords(2); return consumer;
        })) {
            AckableRecord first = requireRecord(source);
            await(() -> consumer.polls.get() >= 3);
            assertEquals(List.of(2), consumer.batchSizes.stream().filter(count -> count > 0).toList());
            first.ack();
            AckableRecord second = requireRecord(source);
            assertEquals("1", second.value());
            assertEquals(List.of(2), consumer.batchSizes.stream().filter(count -> count > 0).toList());
            second.ack();
            assertEquals("2", requireRecord(source).value());
            assertEquals(List.of(2, 2), consumer.batchSizes.stream().filter(count -> count > 0).toList());
        }
    }

    @Test
    void commitAuthenticationAuthorizationAndFencingAreFatalNotRetried() throws Exception {
        for (RuntimeException original : List.of(new AuthenticationException("auth failed"),
                new org.apache.kafka.common.errors.GroupAuthorizationException("denied"),
                new org.apache.kafka.common.errors.FencedInstanceIdException("fenced"))) {
            TrackingConsumer consumer = new TrackingConsumer();
            CountDownLatch laneReady = new CountDownLatch(1);
            consumer.commitHook = () -> {
                try { assertTrue(laneReady.await(1, TimeUnit.SECONDS)); }
                catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
                throw original;
            };
            batch(consumer, row(P0, 0, "done"), row(P0, 1, "unfinished"));
            KafkaSource source = source(consumer, 1);
            AtomicReference<Throwable> laneFailure = new AtomicReference<>();
            AtomicReference<AckableRecord> unfinished = new AtomicReference<>();
            Thread lane = Thread.ofVirtual().start(() -> {
                try { requireRecord(source).ack(); unfinished.set(requireRecord(source)); }
                catch (Throwable failure) { laneFailure.set(failure); }
                finally { laneReady.countDown(); }
            });
            try {
                lane.join(1000); assertFalse(lane.isAlive()); assertNull(laneFailure.get());
                await(consumer::closed);
                assertSame(original, assertThrows(original.getClass(), source::poll));
                assertSame(original, assertThrows(original.getClass(), unfinished.get()::ack));
                assertTrue(consumer.commits.stream().noneMatch(map -> map.get(P0).offset() > 1));
            } finally { laneReady.countDown(); source.close(); lane.interrupt(); lane.join(1000); }
        }
    }

    @Test
    void ownerBudgetsRemainBelowEvenSmallNativeMaxPollInterval() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "done"));
        Connector connector = connector();
        var properties = new java.util.HashMap<>(connector.getProperties()); properties.put("max.poll.interval.ms", 1);
        connector.setProperties(properties);
        try (KafkaSource source = new KafkaSource(connector, List.of("input"), "short-budget", 1, config -> consumer)) {
            requireRecord(source).ack();
            await(() -> consumer.hasCommit(P0, 1));
            assertTrue(consumer.lastPollTimeout().toNanos() < TimeUnit.MILLISECONDS.toNanos(1));
            assertTrue(consumer.commitTimeouts.stream().allMatch(timeout -> timeout.toNanos() < TimeUnit.MILLISECONDS.toNanos(1)));
        }
    }

    @Test
    void finalFlushIsCappedToPositiveRemainingCloseBudget() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "done"), row(P0, 1, "unfinished"), row(P0, 2, "queued"));
        Duration closeTimeout = Duration.ofMillis(300);
        KafkaSource source = new KafkaSource(connector(), List.of("input"), "close-positive", 10000,
            config -> consumer, closeTimeout);
        CountDownLatch ownerHeld = new CountDownLatch(1);
        CountDownLatch releaseOwner = new CountDownLatch(1);
        Thread closer = null;
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        long remainingUpperBound = 0;
        try {
            requireRecord(source).ack();
            requireRecord(source);
            consumer.pollHook = () -> {
                ownerHeld.countDown();
                try { assertTrue(releaseOwner.await(2, TimeUnit.SECONDS)); }
                catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
            };
            assertTrue(ownerHeld.await(1, TimeUnit.SECONDS));
            closer = Thread.ofVirtual().start(() -> {
                try { source.close(); } catch (Throwable failure) { closeFailure.set(failure); }
            });
            assertTrue(consumer.wakeupCalled.await(1, TimeUnit.SECONDS));
            remainingUpperBound = closeTimeout.toNanos() - (System.nanoTime() - consumer.wakeupNanos);
            assertTrue(remainingUpperBound > 0);
            releaseOwner.countDown();
            closer.join(1000);
            assertFalse(closer.isAlive());
            assertNull(closeFailure.get());
            assertTrue(consumer.closed());
            assertEquals(1, consumer.attempts.get());
            assertEquals(Map.of(P0, new OffsetAndMetadata(1)), consumer.commits.getFirst());
            assertTrue(consumer.commitTimeouts.getFirst().toNanos() <= remainingUpperBound,
                "final flush must use only the remaining aggregate close budget");
        } finally {
            releaseOwner.countDown(); source.close();
            if (closer != null) { closer.join(1000); assertFalse(closer.isAlive()); }
            await(consumer::closed);
            Thread owner = consumer.calls.getFirst(); owner.join(1000); assertFalse(owner.isAlive());
        }
    }

    @Test
    void expiredCloseBudgetSkipsFinalCommitAndClosesWithZeroTimeout() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "done"), row(P0, 1, "unfinished"));
        KafkaSource source = new KafkaSource(connector(), List.of("input"), "close-expired", 10000,
            config -> consumer, Duration.ofMillis(40));
        CountDownLatch ownerHeld = new CountDownLatch(1);
        CountDownLatch releaseOwner = new CountDownLatch(1);
        Thread closer = null;
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        try {
            requireRecord(source).ack();
            requireRecord(source);
            consumer.pollHook = () -> {
                ownerHeld.countDown();
                try { assertTrue(releaseOwner.await(2, TimeUnit.SECONDS)); }
                catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
            };
            assertTrue(ownerHeld.await(1, TimeUnit.SECONDS));
            closer = Thread.ofVirtual().start(() -> {
                try { source.close(); } catch (Throwable failure) { closeFailure.set(failure); }
            });
            assertTrue(consumer.wakeupCalled.await(1, TimeUnit.SECONDS));
            closer.join(1000);
            assertFalse(closer.isAlive(), "caller must finish when its aggregate close deadline expires");
            assertNull(closeFailure.get());
            assertFalse(consumer.closed(), "owner remains on test latch until released");
            assertEquals(0, consumer.attempts.get());
            releaseOwner.countDown();
            await(consumer::closed);
            assertEquals(0, consumer.attempts.get(), "expired close deadline must skip the final commit RPC");
            assertEquals(List.of(Duration.ZERO), consumer.closeTimeouts);
        } finally {
            releaseOwner.countDown(); source.close();
            if (closer != null) { closer.join(1000); assertFalse(closer.isAlive()); }
            await(consumer::closed);
            Thread owner = consumer.calls.getFirst(); owner.join(1000); assertFalse(owner.isAlive());
        }
    }

    @Test
    void ownerOnlyFailureStartsCloseDeadlineBeforeItsFinalFlush() throws Exception {
        TrackingConsumer consumer = new TrackingConsumer();
        batch(consumer, row(P0, 0, "done"), row(P0, 1, "unfinished"));
        Duration closeTimeout = Duration.ofMillis(300);
        KafkaSource source = new KafkaSource(connector(), List.of("input"), "owner-close", 10000,
            config -> consumer, closeTimeout);
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        Thread lane = Thread.ofVirtual().start(() -> {
            try { requireRecord(source).ack(); requireRecord(source); }
            catch (Throwable failure) { workerFailure.set(failure); }
        });
        try {
            lane.join(1000); assertFalse(lane.isAlive()); assertNull(workerFailure.get());
            IllegalStateException original = new IllegalStateException("owner-only teardown");
            consumer.pollHook = () -> { throw original; };
            await(consumer::closed);
            assertEquals(1, consumer.attempts.get());
            assertTrue(consumer.commitTimeouts.getFirst().compareTo(closeTimeout) <= 0,
                "owner-only failure final flush must establish and honor its close deadline");
            assertTrue(consumer.closeTimeouts.getFirst().compareTo(closeTimeout) <= 0);
            assertEquals(Map.of(P0, new OffsetAndMetadata(1)), consumer.commits.getFirst());
            assertSame(original, assertThrows(IllegalStateException.class, source::poll));
        } finally {
            source.close(); lane.interrupt(); lane.join(1000); assertFalse(lane.isAlive());
            Thread owner = consumer.calls.getFirst(); owner.join(1000); assertFalse(owner.isAlive());
        }
    }

    private static final class TrackingConsumer extends MockConsumer<String, String> {
        final List<Thread> calls = new CopyOnWriteArrayList<>();
        final List<Map<TopicPartition, OffsetAndMetadata>> commits = new CopyOnWriteArrayList<>();
        final AtomicInteger polls = new AtomicInteger();
        final List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        final List<Duration> commitTimeouts = new CopyOnWriteArrayList<>();
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger wakeups = new AtomicInteger();
        final CountDownLatch wakeupCalled = new CountDownLatch(1);
        final List<Duration> closeTimeouts = new CopyOnWriteArrayList<>();
        volatile long wakeupNanos;
        volatile Runnable pollHook;
        volatile Runnable commitHook;
        final CountDownLatch commitEntered = new CountDownLatch(1);
        final CountDownLatch releaseCommit = new CountDownLatch(1);
        volatile boolean blockCommit;
        volatile RuntimeException commitFailure;
        volatile RuntimeException subscribeFailure;
        volatile Thread createdBy;
        TrackingConsumer() { super("earliest"); }
        private void call() { calls.add(Thread.currentThread()); }
        public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
            call(); if (subscribeFailure != null) throw subscribeFailure; super.subscribe(topics, listener);
        }
        public org.apache.kafka.clients.consumer.ConsumerRecords<String, String> poll(Duration timeout) {
            call(); polls.incrementAndGet();
            Runnable hook = pollHook; pollHook = null;
            if (hook != null) hook.run();
            var records = super.poll(timeout); batchSizes.add(records.count()); return records;
        }
        public Set<TopicPartition> assignment() { call(); return super.assignment(); }
        public void pause(Collection<TopicPartition> partitions) { call(); super.pause(partitions); }
        public void resume(Collection<TopicPartition> partitions) { call(); super.resume(partitions); }
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
            call(); attempts.incrementAndGet(); commitTimeouts.add(timeout); assertTrue(timeout.toMillis() <= 1000);
            Runnable hook = commitHook; commitHook = null;
            if (hook != null) hook.run();
            RuntimeException failure = commitFailure; commitFailure = null;
            if (failure != null) throw failure;
            if (blockCommit) {
                blockCommit = false; commitEntered.countDown();
                try { assertTrue(releaseCommit.await(2, TimeUnit.SECONDS)); }
                catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
            }
            commits.add(Map.copyOf(offsets)); super.commitSync(offsets, timeout);
        }
        public void commitSync() { fail("parameterless commit is unsafe"); }
        public void commitSync(Duration timeout) { fail("position commit is unsafe"); }
        public void wakeup() {
            wakeupNanos = System.nanoTime(); wakeups.incrementAndGet(); super.wakeup(); wakeupCalled.countDown();
        }
        public void close(Duration timeout) { call(); closeTimeouts.add(timeout); super.close(timeout); }
        boolean hasCommit(TopicPartition partition, long offset) {
            return commits.stream().anyMatch(map -> map.containsKey(partition) && map.get(partition).offset() == offset);
        }
    }
}
