package com.tencent.cloud.mqtt.kafka;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.cloud.mqtt.AckableRecord;
import com.tencent.cloud.mqtt.Source;
import com.tencent.cloud.mqtt.model.Connector;

public final class KafkaSource implements Source {
    public static final long DEFAULT_COMMIT_INTERVAL_MILLIS = 1000;
    private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);
    private static final long CLOSE_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final Object monitor = new Object();
    private final ArrayDeque<CompletionToken> buffered = new ArrayDeque<>();
    private final Map<TopicPartition, Lease> leases = new HashMap<>();
    private final CompletableFuture<Void> started = new CompletableFuture<>();
    private final Thread owner;
    private final Duration pollBudget;
    private final Duration commitBudget;
    private final long commitIntervalNanos;
    private final long closeTimeoutNanos;
    private volatile Consumer<String, String> consumer;
    private CompletionToken active;
    private Thread taskThread;
    private boolean stopping;
    private Throwable failure;
    private long closeDeadline;

    private static final class Lease {
        boolean valid = true;
        OffsetAndMetadata completed;
        long committed = -1;
    }

    static final class CompletionToken {
        final ConsumerRecord<String, String> record;
        final TopicPartition partition;
        final Lease lease;
        CompletionToken(ConsumerRecord<String, String> record, Lease lease) {
            this.record = record;
            this.partition = new TopicPartition(record.topic(), record.partition());
            this.lease = lease;
        }
    }

    private record CommitPoint(Lease lease, OffsetAndMetadata offset) {}

    public KafkaSource(Connector connector, List<String> topics, String clientId) {
        this(connector, topics, clientId, DEFAULT_COMMIT_INTERVAL_MILLIS);
    }

    public KafkaSource(Connector connector, List<String> topics, String clientId, long commitIntervalMillis) {
        this(connector, topics, clientId, commitIntervalMillis, KafkaConsumer::new);
    }

    KafkaSource(Connector connector, List<String> topics, String clientId, long commitIntervalMillis,
            Function<Map<String, Object>, Consumer<String, String>> factory) {
        this(connector, topics, clientId, commitIntervalMillis, factory, Duration.ofNanos(CLOSE_NANOS));
    }

    KafkaSource(Connector connector, List<String> topics, String clientId, long commitIntervalMillis,
            Function<Map<String, Object>, Consumer<String, String>> factory, Duration closeTimeout) {
        closeTimeoutNanos = closeTimeout.toNanos();
        validateTopics(topics);
        List<String> subscription = List.copyOf(topics);
        if (commitIntervalMillis <= 0) {
            throw new IllegalArgumentException("Kafka source commit_interval_ms must be positive");
        }
        Map<String, Object> config = KafkaClients.consumerConfig(connector, clientId);
        int maxPollMillis = new ConsumerConfig(config).getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        long membershipBudgetNanos = TimeUnit.MILLISECONDS.toNanos(maxPollMillis) / 4;
        pollBudget = Duration.ofNanos(Math.min(TimeUnit.MILLISECONDS.toNanos(100), membershipBudgetNanos));
        commitBudget = Duration.ofNanos(Math.min(TimeUnit.SECONDS.toNanos(1), membershipBudgetNanos));
        commitIntervalNanos = TimeUnit.MILLISECONDS.toNanos(commitIntervalMillis);
        owner = Thread.ofVirtual().name("kafka-owner-" + clientId).unstarted(() -> runOwner(factory, config, subscription));
        owner.start();
        try {
            started.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            close();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted starting Kafka source", interrupted);
        } catch (ExecutionException failed) {
            close();
            throw propagate(failed.getCause());
        } catch (java.util.concurrent.TimeoutException timeout) {
            close();
            throw new IllegalStateException("Timed out starting Kafka source", timeout);
        }
    }

    public static void validateTopics(List<String> topics) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("Kafka source requires at least one topic");
        }
        topics.forEach(KafkaRecordMapper::validateTopic);
    }

    @Override
    public AckableRecord poll() throws InterruptedException {
        synchronized (monitor) {
            taskThread = Thread.currentThread();
            for (;;) {
                checkFailure();
                if (stopping) return null;
                if (Thread.interrupted()) throw new InterruptedException();
                if (active == null && !buffered.isEmpty()) {
                    CompletionToken token = buffered.removeFirst();
                    active = token;
                    if (token.record.value() == null) {
                        log.warn("Skipping Kafka tombstone {}:{}:{}", token.record.topic(),
                            token.record.partition(), token.record.offset());
                        complete(token);
                        continue;
                    }
                    return new KafkaAckableRecord(KafkaRecordMapper.toRecord(token.record), this, token);
                }
                monitor.wait();
            }
        }
    }

    void complete(CompletionToken token) {
        synchronized (monitor) {
            checkFailure();
            if (active != token) return;
            if (!stopping && token.lease.valid && leases.get(token.partition) == token.lease) {
                token.lease.completed = new OffsetAndMetadata(token.record.offset() + 1,
                    token.record.leaderEpoch(), "");
            } else {
                log.warn("Ignoring stale Kafka completion {}:{}:{}", token.record.topic(),
                    token.record.partition(), token.record.offset());
            }
            active = null;
            monitor.notifyAll();
        }
    }

    private void runOwner(Function<Map<String, Object>, Consumer<String, String>> factory,
            Map<String, Object> config, List<String> topics) {
        try {
            consumer = factory.apply(config);
            if (isStopping()) return;
            consumer.subscribe(topics, new ConsumerRebalanceListener() {
                @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    revoke(partitions, false);
                }
                @Override public void onPartitionsLost(Collection<TopicPartition> partitions) {
                    revoke(partitions, true);
                }
                @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    synchronized (monitor) {
                        if (stopping) return;
                        partitions.forEach(partition -> leases.computeIfAbsent(partition, ignored -> new Lease()));
                    }
                    updatePause();
                }
            });
            started.complete(null);
            long lastCommit = System.nanoTime();
            while (!isStopping()) {
                long tick = System.nanoTime();
                updatePause();
                var records = consumer.poll(pollBudget);
                synchronized (monitor) {
                    if (!stopping) {
                        for (var record : records) {
                            Lease lease = leases.get(new TopicPartition(record.topic(), record.partition()));
                            if (lease != null && lease.valid) buffered.addLast(new CompletionToken(record, lease));
                        }
                        monitor.notifyAll();
                    }
                }
                updatePause();
                long now = System.nanoTime();
                if (!isStopping() && now - lastCommit >= commitIntervalNanos) {
                    commit(snapshot(null), false);
                    lastCommit = System.nanoTime();
                }
                waitForCadence(tick);
            }
        } catch (WakeupException wakeup) {
            if (!isStopping()) fail(wakeup);
        } catch (Throwable fatal) {
            fail(fatal);
        } finally {
            if (!started.isDone()) {
                started.completeExceptionally(failure == null
                    ? new IllegalStateException("Kafka source stopped during startup") : failure);
            }
            if (consumer != null) {
                try {
                    Duration remaining = remainingCloseBudget();
                    if (!remaining.isZero()) {
                        commit(snapshot(null), true, remaining.compareTo(commitBudget) < 0 ? remaining : commitBudget);
                    }
                } catch (Throwable flushFailure) { log.warn("Kafka source final completed-offset flush failed", flushFailure); }
                try { consumer.close(remainingCloseBudget()); }
                catch (Throwable closeFailure) { log.warn("Kafka source consumer close failed", closeFailure); }
            }
            synchronized (monitor) { monitor.notifyAll(); }
        }
    }

    private void waitForCadence(long tick) throws InterruptedException {
        long deadline = tick + pollBudget.toNanos();
        synchronized (monitor) {
            long remaining;
            while (!stopping && (remaining = deadline - System.nanoTime()) > 0) {
                TimeUnit.NANOSECONDS.timedWait(monitor, remaining);
            }
        }
    }

    private boolean isStopping() {
        synchronized (monitor) { return stopping; }
    }

    private void updatePause() {
        boolean outstanding;
        synchronized (monitor) {
            if (stopping) return;
            outstanding = active != null || !buffered.isEmpty();
        }
        var assignment = consumer.assignment();
        if (assignment.isEmpty()) return;
        if (outstanding) consumer.pause(assignment);
        else consumer.resume(assignment);
    }

    private Map<TopicPartition, CommitPoint> snapshot(Collection<TopicPartition> partitions) {
        Map<TopicPartition, CommitPoint> snapshot = new HashMap<>();
        synchronized (monitor) {
            leases.forEach((partition, lease) -> {
                if (lease.valid && (partitions == null || partitions.contains(partition))
                        && lease.completed != null && lease.completed.offset() > lease.committed) {
                    snapshot.put(partition, new CommitPoint(lease, lease.completed));
                }
            });
        }
        return snapshot;
    }

    private void revoke(Collection<TopicPartition> partitions, boolean lost) {
        Map<TopicPartition, CommitPoint> completed;
        synchronized (monitor) {
            completed = !stopping && !lost ? snapshot(partitions) : Map.of();
            for (TopicPartition partition : partitions) {
                Lease removed = leases.remove(partition);
                if (removed != null) removed.valid = false;
            }
            buffered.removeIf(token -> partitions.contains(token.partition));
            monitor.notifyAll();
        }
        if (!completed.isEmpty()) commit(completed, true);
    }

    private void commit(Map<TopicPartition, CommitPoint> snapshot, boolean bestEffort) {
        commit(snapshot, bestEffort, commitBudget);
    }

    private void commit(Map<TopicPartition, CommitPoint> snapshot, boolean bestEffort, Duration timeout) {
        if (snapshot.isEmpty()) return;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        snapshot.forEach((partition, point) -> offsets.put(partition, point.offset()));
        try {
            consumer.commitSync(offsets, timeout);
            synchronized (monitor) {
                snapshot.forEach((partition, point) -> {
                    if (point.lease().valid && leases.get(partition) == point.lease()) {
                        point.lease().committed = Math.max(point.lease().committed, point.offset().offset());
                    }
                });
            }
        } catch (AuthenticationException | AuthorizationException | FencedInstanceIdException fatal) {
            throw fatal;
        } catch (RuntimeException commitFailure) {
            if (bestEffort || commitFailure instanceof RetriableException
                    || commitFailure instanceof RebalanceInProgressException
                    || commitFailure instanceof CommitFailedException) {
                log.warn("Kafka completed-offset commit failed; retaining uncommitted completions", commitFailure);
            } else {
                throw commitFailure;
            }
        }
    }

    private void fail(Throwable fatal) {
        Thread lane;
        synchronized (monitor) {
            if (failure == null) failure = fatal;
            lane = taskThread;
            monitor.notifyAll();
        }
        log.error("Kafka source owner failed", fatal);
        started.completeExceptionally(fatal);
        if (lane != null) lane.interrupt();
    }

    private void checkFailure() {
        if (failure != null) throw propagate(failure);
    }

    private static RuntimeException propagate(Throwable failure) {
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException runtime) return runtime;
        return new IllegalStateException("Kafka source owner failed", failure);
    }

    private Duration remainingCloseBudget() {
        long deadline;
        synchronized (monitor) {
            if (closeDeadline == 0) closeDeadline = System.nanoTime() + closeTimeoutNanos;
            deadline = closeDeadline;
        }
        return Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
    }

    @Override
    public void close() {
        boolean wakeOwner = false;
        synchronized (monitor) {
            if (!stopping) {
                wakeOwner = true;
                stopping = true;
                taskThread = null;
                closeDeadline = System.nanoTime() + closeTimeoutNanos;
                buffered.clear();
                monitor.notifyAll();
            }
        }
        Consumer<String, String> current = consumer;
        if (wakeOwner && owner.isAlive() && current != null) current.wakeup();
        if (Thread.currentThread() == owner) return;
        boolean interrupted = Thread.interrupted();
        try {
            while (owner.isAlive()) {
                long remaining = remainingCloseBudget().toNanos();
                if (remaining <= 0) break;
                try { TimeUnit.NANOSECONDS.timedJoin(owner, remaining); }
                catch (InterruptedException interruption) { interrupted = true; }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
        if (owner.isAlive()) log.warn("Kafka source owner did not stop within close budget");
    }
}
