package com.tencent.cloud.mqtt.kafka;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.Executable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.cloud.mqtt.SQLTransform;
import com.tencent.cloud.mqtt.Sink;
import com.tencent.cloud.mqtt.Task;
import com.tencent.cloud.mqtt.model.Connector;
import com.tencent.cloud.mqtt.model.ConnectorType;

@Testcontainers
@Timeout(120)
class KafkaSourceIntegrationTest {

    @Container
    static final AuthenticatedKafkaContainer KAFKA = new AuthenticatedKafkaContainer();

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SQL =
        "SELECT payload.device AS device, payload.temp * 2 AS doubled FROM payload WHERE payload.temp > 20";
    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final Duration NEGATIVE_WINDOW = Duration.ofSeconds(3);
    private static final byte[] BINARY_HEADER = {0, (byte) 0xff, 42, (byte) 0x80};

    private final AtomicReference<Throwable> taskFailure = new AtomicReference<>();
    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final List<GateSink> gates = new ArrayList<>();
    private AdminClient admin;
    private KafkaProducer<String, String> publisher;
    private KafkaConsumer<String, String> verifier;
    private KafkaSource source;
    private Thread taskThread;
    private KafkaSource secondSource;
    private Thread secondTaskThread;
    private final AtomicReference<Throwable> secondTaskFailure = new AtomicReference<>();
    private final List<RebalanceGateSink> rebalanceGates = new ArrayList<>();

    @BeforeAll
    static void verifyAuthenticatedReadiness() throws Exception {
        try (AdminClient ready = AdminClient.create(KAFKA.clientProperties(Map.of(
                "default.api.timeout.ms", 10_000, "request.timeout.ms", 5_000)))) {
            assertFalse(ready.describeCluster().nodes().get(10, TimeUnit.SECONDS).isEmpty());
            ready.listTopics().names().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void adminRejectsIncorrectPasswordWithNativeAuthenticationFailure() throws Exception {
        Map<String, Object> properties = KAFKA.badPasswordProperties();
        properties.put("default.api.timeout.ms", 10_000);
        properties.put("request.timeout.ms", 5_000);
        try (AdminClient rejected = AdminClient.create(properties)) {
            ExecutionException failure = assertThrows(ExecutionException.class,
                () -> rejected.describeCluster().nodes().get(10, TimeUnit.SECONDS));
            assertAuthenticationCause(failure);
        }
    }

    @Test
    void sinkPublishRejectsIncorrectPasswordWithNativeAuthenticationFailure() throws Exception {
        String suffix = suffix();
        String topic = "it-auth-sink-" + suffix;
        createTopics(topic);
        Map<String, Object> properties = KAFKA.badPasswordProperties();
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        Connector connector = new Connector();
        connector.setType(ConnectorType.Kafka);
        connector.setProperties(properties);
        KafkaSink rejected = new KafkaSink(connector, topic, "rejected-sink-" + suffix);
        try {
            AuthenticationException failure = assertThrows(AuthenticationException.class,
                () -> rejected.publish(new Record<>("key", payload("auth", 25), System.currentTimeMillis())));
            assertAuthenticationCause(failure);
        } finally {
            rejected.close();
        }
    }

    @Test
    void sourcePollRejectsIncorrectPasswordAndPreservesOwnerAuthenticationFailure() throws Exception {
        String suffix = suffix();
        String topic = "it-auth-source-" + suffix;
        createTopics(topic);
        Connector connector = connector("it-auth-group-" + suffix, KAFKA.badPasswordProperties());
        connector.getProperties().put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);
        connector.getProperties().put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        AtomicReference<Throwable> observedFailure = new AtomicReference<>();
        AtomicReference<KafkaSource> rejected = new AtomicReference<>();
        CountDownLatch stopped = new CountDownLatch(1);
        Thread worker = Thread.ofVirtual().name("rejected-source-" + suffix).start(() -> {
            try {
                KafkaSource consumer = new KafkaSource(connector, List.of(topic), "rejected-" + suffix, 100);
                rejected.set(consumer);
                try {
                    consumer.poll();
                } catch (InterruptedException interrupted) {
                    // The owner interrupts a waiting poll; its stored failure is checked first on the next poll.
                    Thread.interrupted();
                    try {
                        consumer.poll();
                    } finally {
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (Throwable failure) {
                observedFailure.set(failure);
            } finally {
                KafkaSource consumer = rejected.get();
                if (consumer != null) {
                    consumer.close();
                }
                stopped.countDown();
            }
        });
        try {
            assertTrue(stopped.await(15, TimeUnit.SECONDS), "bad-password source must fail within the bounded window");
            assertAuthenticationCause(observedFailure.get());
        } finally {
            KafkaSource consumer = rejected.get();
            if (consumer != null) {
                consumer.close();
            }
            worker.interrupt();
            worker.join(15_000);
            assertFalse(worker.isAlive(), "rejected source worker and its owner must close");
            assertFalse(Thread.currentThread().isInterrupted(), "source failure must not interrupt the test thread");
        }
    }

    private static void assertAuthenticationCause(Throwable failure) {
        assertNotNull(failure, "operation must fail rather than accept invalid credentials");
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SaslAuthenticationException) {
                return;
            }
        }
        fail("expected native SaslAuthenticationException in the original cause chain", failure);
    }

    @Test
    void kafkaToKafkaTransformsBatchAndCommitsFilteredAndTombstoneOffsets() throws Exception {
        String suffix = suffix();
        String input = "it-source-" + suffix + "-input";
        String output = "it-source-" + suffix + "-default";
        String routed = "it-source-" + suffix + "-routed";
        String group = "it-source-" + suffix;
        createTopics(input, output, routed);
        createVerifier(output, routed);
        createPublisher();
        long timestamp = System.currentTimeMillis() - 60_000;
        send(input, "filtered", payload("filtered", 5), timestamp, new RecordHeaders());
        RecordMetadata defaultInput = send(input, "key-default", payload("default", 25), timestamp,
            richHeaders(null));
        send(input, "tombstone", null, timestamp, new RecordHeaders());
        RecordMetadata routedInput = send(input, "key-routed", payload("routed", 30), timestamp + 1,
            richHeaders(routed));
        send(input, "over-limit", payload("over-limit", 35), timestamp,
            new RecordHeaders().add(Task.HOP_COUNT_HEADER, utf8("2")));
        send(input, "malformed", "{malformed-json", timestamp, new RecordHeaders());
        RecordMetadata lastInput = send(input, "poison-routing", payload("poison-routing", 45), timestamp,
            new RecordHeaders().add("dst.kafka.topic", utf8("invalid/topic")));

        // The same native properties can be reused by the source and sink connectors.
        Connector connector = connector(group, Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20));
        source = new KafkaSource(connector, List.of(input), "source-" + suffix, 100);
        startTask(new KafkaSink(connector, output, "sink-" + suffix));

        List<ConsumerRecord<String, String>> records = receiveRecords(2);
        ConsumerRecord<String, String> defaultRecord = onTopic(records, output);
        ConsumerRecord<String, String> routedRecord = onTopic(records, routed);
        assertOutput(defaultRecord, "default", 50, "key-default", timestamp, input, defaultInput);
        assertOutput(routedRecord, "routed", 60, "key-routed", timestamp + 1, input, routedInput);
        for (ConsumerRecord<String, String> record : records) {
            assertEquals(List.of("blue", "green"), headerValues(record, "label"));
            assertArrayEquals(BINARY_HEADER, record.headers().lastHeader("binary").value());
            assertNotNull(record.headers().lastHeader("nullable"));
            assertNull(record.headers().lastHeader("nullable").value());
            assertEquals("upstream-message", header(record, "$__messageId"));
            assertEquals("upstream-publisher", header(record, "$__publisherClientId"));
            assertEquals("upstream/mqtt", header(record, "src.mqtt.topic"));
            assertEquals("upstream-rmq", header(record, "src.rmq.topic"));
            assertNull(record.headers().lastHeader("dst.kafka.topic"), "routing override is consumed");
        }
        awaitCommitted(group, new TopicPartition(input, 0), lastInput.offset() + 1);
        assertNoMoreRecords();
    }

    @Test
    void commitsOnlyAfterSinkCompletionAndRestartReplaysOnlyUnfinishedInput() throws Exception {
        String suffix = suffix();
        String input = "it-offsets-" + suffix;
        String group = "it-offsets-group-" + suffix;
        TopicPartition partition = new TopicPartition(input, 0);
        createTopics(input);
        createPublisher();
        send(input, "first", payload("first", 25), System.currentTimeMillis(), new RecordHeaders());
        send(input, "buffered-tombstone", null, System.currentTimeMillis(), new RecordHeaders());
        RecordMetadata second = send(input, "second", payload("second", 30),
            System.currentTimeMillis(), new RecordHeaders());
        Connector connector = connector(group, Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20));
        GateSink firstSink = gate();
        source = new KafkaSource(connector, List.of(input), "source-first-" + suffix, 100);
        startTask(firstSink);
        assertTrue(firstSink.entered.await(30, TimeUnit.SECONDS), "first input must reach the blocked sink");
        assertEquals("first", firstSink.active.get().key());
        awaitAssigned(group, partition);
        assertNotCommittedPast(group, partition, 0, Duration.ofSeconds(1));
        assertTrue(firstSink.published.isEmpty(), "unfinished input has not been published");

        firstSink.release.countDown();
        assertEquals("first", receivePublished(firstSink).key());
        assertEquals("second", receivePublished(firstSink).key());
        awaitCommitted(group, partition, second.offset() + 1);
        stopTask();

        GateSink unfinishedSink = gate();
        source = new KafkaSource(connector, List.of(input), "source-restart-" + suffix, 100);
        startTask(unfinishedSink);
        awaitAssigned(group, partition);
        RecordMetadata unfinished = send(input, "unfinished", payload("unfinished", 40),
            System.currentTimeMillis(), new RecordHeaders());
        assertTrue(unfinishedSink.entered.await(30, TimeUnit.SECONDS), "new input must reach restarted source");
        assertEquals("unfinished", unfinishedSink.active.get().key(),
            "restart must resume after committed inputs rather than replaying them");
        assertNotCommittedPast(group, partition, unfinished.offset(), Duration.ofSeconds(1));
        stopTask(); // Interrupt the sink before releasing it, so this record remains unfinished.
        assertEquals(unfinished.offset(), committed(group, partition));
        assertTrue(unfinishedSink.published.isEmpty());

        GateSink replaySink = gate();
        replaySink.release.countDown();
        source = new KafkaSource(connector, List.of(input), "source-replay-" + suffix, 100);
        startTask(replaySink);
        Record<String, String> replay = receivePublished(replaySink);
        assertNotNull(replay, "unfinished input must be replayed from the broker after restart");
        assertEquals("unfinished", replay.key());
        assertEquals(Long.toString(unfinished.offset()),
            new String(replay.headers().lastHeader("kafka.offset").value(), StandardCharsets.UTF_8));
        awaitCommitted(group, partition, unfinished.offset() + 1);
        long deadline = System.nanoTime() + NEGATIVE_WINDOW.toNanos();
        while (System.nanoTime() < deadline) {
            assertTaskHealthy();
            assertNull(replaySink.published.poll(250, TimeUnit.MILLISECONDS),
                "committed inputs and the completed replay must not be duplicated on clean restart");
        }
    }

    @Test
    void maintainsConsumerMembershipWhileTaskProcessingExceedsMaxPollInterval() throws Exception {
        String suffix = suffix();
        String input = "it-membership-" + suffix;
        String group = "it-membership-group-" + suffix;
        TopicPartition partition = new TopicPartition(input, 0);
        createTopics(input);
        createPublisher();
        send(input, "slow", payload("slow", 25), System.currentTimeMillis(), new RecordHeaders());
        int maxPollIntervalMillis = 3_000;
        Connector connector = connector(group, Map.of(
            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMillis,
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6_000,
            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1_000,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10));
        GateSink sink = gate();
        source = new KafkaSource(connector, List.of(input), "source-slow-" + suffix, 100);
        startTask(sink);
        assertTrue(sink.entered.await(30, TimeUnit.SECONDS), "input must reach the blocked sink");
        MemberDescription initial = awaitAssigned(group, partition);
        long deadline = System.nanoTime() + Duration.ofMillis(maxPollIntervalMillis * 2L + 500).toNanos();
        do {
            assertTaskHealthy();
            ConsumerGroupDescription description = describeGroup(group);
            assertEquals(1, description.members().size(), "paused owner must retain group membership");
            MemberDescription current = description.members().iterator().next();
            assertEquals(initial.consumerId(), current.consumerId(), "processing must not force a rejoin");
            assertEquals(Set.of(partition), current.assignment().topicPartitions());
            assertTrue(committed(group, partition) <= 0, "blocked sink must not advance broker offsets");
            Thread.sleep(100);
        } while (System.nanoTime() < deadline);
        assertTrue(sink.published.isEmpty());
        sink.release.countDown();
        assertNotNull(receivePublished(sink));
        awaitCommitted(group, partition, 1);
        assertTaskHealthy();
    }

    @Test
    void realRebalanceFencesStaleCompletionAndReplaysMovedPartition() throws Exception {
        String suffix = suffix();
        String input = "it-rebalance-" + suffix + "-input";
        String output = "it-rebalance-" + suffix + "-output";
        String group = "it-rebalance-group-" + suffix;
        String firstClient = "rebalance-first-" + suffix;
        String secondClient = "rebalance-second-" + suffix;
        TopicPartition retained = new TopicPartition(input, 0);
        TopicPartition moved = new TopicPartition(input, 1);
        createTopics(output);
        admin.createTopics(List.of(new NewTopic(input, 2, (short) 1)
            .configs(Map.of("message.timestamp.type", "CreateTime")))).all().get(30, TimeUnit.SECONDS);
        createVerifier(output);
        createPublisher();
        Map<String, Object> overrides = Map.of(
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(DeterministicRebalanceAssignor.class.getName()),
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10,
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6_000,
            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1_000);
        Connector connector = connector(group, overrides);
        source = new KafkaSource(connector, List.of(input), firstClient, 100);
        RebalanceGateSink oldSink = new RebalanceGateSink(new KafkaSink(connector, output, "old-sink-" + suffix), "moving");
        rebalanceGates.add(oldSink);
        startTask(oldSink);
        awaitMemberAssignment(group, firstClient, Set.of(retained, moved), 1);
        long timestamp = System.currentTimeMillis() - 60_000;
        RecordMetadata retainedBefore = sendPartition(input, 0, "before-rebalance", payload("before-rebalance", 22),
            timestamp - 1, new RecordHeaders());
        assertOutput(receiveRecords(1).getFirst(), "before-rebalance", 44, "before-rebalance",
            timestamp - 1, input, retainedBefore);
        awaitCommitted(group, retained, retainedBefore.offset() + 1);
        RecordMetadata unfinished = sendPartition(input, 1, "moving", payload("moving", 25), timestamp, new RecordHeaders());
        assertTrue(oldSink.entered.await(30, TimeUnit.SECONDS), "old owner must stall the partition selected to move");
        assertEquals("moving", oldSink.active.get().key());
        assertEquals("1", new String(oldSink.active.get().headers().lastHeader("kafka.partition").value(), StandardCharsets.UTF_8));
        assertNotCommittedPast(group, moved, unfinished.offset(), Duration.ofSeconds(1));

        secondSource = new KafkaSource(connector, List.of(input), secondClient, 100);
        RebalanceGateSink newSink = new RebalanceGateSink(new KafkaSink(connector, output, "new-sink-" + suffix), "moving");
        rebalanceGates.add(newSink);
        secondTaskThread = launchTask(secondSource, newSink, "rebalance-second-task-" + suffix, secondTaskFailure);
        try {
            awaitMemberAssignment(group, firstClient, Set.of(retained), 2);
            awaitMemberAssignment(group, secondClient, Set.of(moved), 2);
            assertTrue(newSink.entered.await(30, TimeUnit.SECONDS), "new owner must receive the uncommitted broker replay");
            assertEquals("moving", newSink.active.get().key());
            assertEquals(Long.toString(unfinished.offset()),
                new String(newSink.active.get().headers().lastHeader("kafka.offset").value(), StandardCharsets.UTF_8));
            assertNotCommittedPast(group, moved, unfinished.offset(), Duration.ofSeconds(1));

            RecordMetadata retainedInput = sendPartition(input, 0, "retained", payload("retained", 30),
                timestamp + 1, new RecordHeaders());
            oldSink.release.countDown();
            List<ConsumerRecord<String, String>> oldAndRetained = receiveRecords(2);
            assertEquals(Set.of("moving", "retained"),
                Set.copyOf(oldAndRetained.stream().map(ConsumerRecord::key).toList()));
            for (ConsumerRecord<String, String> record : oldAndRetained) {
                if (record.key().equals("moving")) {
                    assertOutput(record, "moving", 50, "moving", timestamp, input, unfinished);
                } else {
                    assertOutput(record, "retained", 60, "retained", timestamp + 1, input, retainedInput);
                }
            }
            // Processing and committing p0 proves the old lane already completed its stale p1 work.
            awaitCommitted(group, retained, retainedInput.offset() + 1);
            assertNotCommittedPast(group, moved, unfinished.offset(), NEGATIVE_WINDOW);
            assertTrue(newSink.published.isEmpty(), "new-owner completion is still gated, so stale old ack cannot hide");
            assertEquals(Set.of(moved), awaitMemberAssignment(group, secondClient, Set.of(moved), 2)
                .assignment().topicPartitions());

            newSink.release.countDown();
            ConsumerRecord<String, String> replay = receiveRecords(1).getFirst();
            assertOutput(replay, "moving", 50, "moving", timestamp, input, unfinished);
            awaitCommitted(group, moved, unfinished.offset() + 1);
            RecordMetadata continuing = sendPartition(input, 0, "continuing", payload("continuing", 35),
                timestamp + 2, new RecordHeaders());
            RecordMetadata follower = sendPartition(input, 1, "follower", payload("follower", 40),
                timestamp + 3, new RecordHeaders());
            List<ConsumerRecord<String, String>> progress = receiveRecords(2);
            assertEquals(Set.of("continuing", "follower"), Set.copyOf(progress.stream().map(ConsumerRecord::key).toList()));
            for (ConsumerRecord<String, String> record : progress) {
                if (record.key().equals("continuing")) {
                    assertOutput(record, "continuing", 70, "continuing", timestamp + 2, input, continuing);
                } else {
                    assertOutput(record, "follower", 80, "follower", timestamp + 3, input, follower);
                }
            }
            awaitCommitted(group, retained, continuing.offset() + 1);
            awaitCommitted(group, moved, follower.offset() + 1);
            assertNoMoreRecords();
        } finally {
            oldSink.release.countDown();
            newSink.release.countDown();
        }
    }

    @Test
    void readCommittedSkipsAbortedTransactionAndPreservesNativeOffsets() throws Exception {
        String suffix = suffix();
        String input = "it-transactions-" + suffix + "-input";
        String output = "it-transactions-" + suffix + "-output";
        String group = "it-transactions-group-" + suffix;
        createTopics(input, output);
        createVerifier(output);
        createPublisher();
        Map<String, Object> transactionalProperties = KAFKA.clientProperties(Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, "it-transaction-" + suffix,
            ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 30_000,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000));
        long timestamp = System.currentTimeMillis() - 60_000;
        RecordMetadata aborted;
        RecordMetadata committedInput;
        KafkaProducer<String, String> transactional = new KafkaProducer<>(transactionalProperties);
        try {
            transactional.initTransactions();
            transactional.beginTransaction();
            aborted = transactional.send(new ProducerRecord<>(input, 0, timestamp, "aborted",
                payload("aborted", 25), new RecordHeaders())).get(30, TimeUnit.SECONDS);
            transactional.abortTransaction();
            transactional.beginTransaction();
            committedInput = transactional.send(new ProducerRecord<>(input, 0, timestamp + 1, "committed",
                payload("committed", 30), new RecordHeaders())).get(30, TimeUnit.SECONDS);
            transactional.commitTransaction();
        } finally {
            transactional.close(Duration.ofSeconds(10));
        }
        RecordMetadata sentinel = send(input, "sentinel", payload("sentinel", 35), timestamp + 2, new RecordHeaders());
        assertTrue(committedInput.offset() > aborted.offset(), "aborted transaction occupies native offsets");
        assertTrue(sentinel.offset() > committedInput.offset() + 1, "commit control batch creates an offset gap");
        Connector connector = connector(group, Map.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20));
        assertFalse(connector.getProperties().containsKey(ConsumerConfig.ISOLATION_LEVEL_CONFIG),
            "this flow must exercise KafkaSource's default read_committed, not an explicit override");
        source = new KafkaSource(connector, List.of(input), "transaction-source-" + suffix, 100);
        startTask(new KafkaSink(connector, output, "transaction-sink-" + suffix));
        List<ConsumerRecord<String, String>> records = receiveRecords(2);
        assertEquals(Set.of("committed", "sentinel"), Set.copyOf(records.stream().map(ConsumerRecord::key).toList()));
        for (ConsumerRecord<String, String> record : records) {
            if (record.key().equals("committed")) {
                assertOutput(record, "committed", 60, "committed", timestamp + 1, input, committedInput);
            } else {
                assertOutput(record, "sentinel", 70, "sentinel", timestamp + 2, input, sentinel);
            }
        }
        awaitCommitted(group, new TopicPartition(input, 0), sentinel.offset() + 1);
        assertNoMoreRecords();
    }

    private static String suffix() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static Connector connector(String group, Map<String, Object> overrides) {
        Map<String, Object> properties = KAFKA.clientProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        properties.putAll(overrides);
        Connector connector = new Connector();
        connector.setType(ConnectorType.Kafka);
        connector.setProperties(properties);
        return connector;
    }

    private void createTopics(String... topics) throws Exception {
        admin = AdminClient.create(KAFKA.clientProperties(Map.of(
            "default.api.timeout.ms", 30_000, "request.timeout.ms", 10_000)));
        List<NewTopic> definitions = new ArrayList<>();
        for (String topic : topics) {
            definitions.add(new NewTopic(topic, 1, (short) 1)
                .configs(Map.of("message.timestamp.type", "CreateTime")));
        }
        admin.createTopics(definitions).all().get(30, TimeUnit.SECONDS);
    }

    private void createPublisher() {
        publisher = new KafkaProducer<>(KAFKA.clientProperties(Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000)));
    }

    private RecordMetadata send(String topic, String key, String value, long timestamp, Headers headers)
            throws Exception {
        return publisher.send(new ProducerRecord<>(topic, 0, timestamp, key, value, headers))
            .get(30, TimeUnit.SECONDS);
    }

    private RecordMetadata sendPartition(String topic, int partition, String key, String value,
            long timestamp, Headers headers) throws Exception {
        return publisher.send(new ProducerRecord<>(topic, partition, timestamp, key, value, headers))
            .get(30, TimeUnit.SECONDS);
    }

    private static String payload(String device, int temperature) {
        return "{\"device\":\"" + device + "\",\"temp\":" + temperature + "}";
    }

    private static Headers richHeaders(String routedTopic) {
        Headers headers = new RecordHeaders()
            .add("label", utf8("blue")).add("label", utf8("green"))
            .add("binary", BINARY_HEADER).add("nullable", null)
            .add("$__messageId", utf8("upstream-message"))
            .add("$__publisherClientId", utf8("upstream-publisher"))
            .add("src.mqtt.topic", utf8("upstream/mqtt")).add("src.rmq.topic", utf8("upstream-rmq"))
            .add("src.kafka.topic", utf8("forged-source")).add("src.kafka.topic", utf8("forged-again"))
            .add("kafka.partition", utf8("999")).add("kafka.offset", utf8("999"));
        if (routedTopic != null) {
            headers.add("dst.kafka.topic", utf8(routedTopic));
        }
        return headers;
    }

    private void createVerifier(String... topics) {
        verifier = new KafkaConsumer<>(KAFKA.clientProperties(Map.of(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000,
            ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000)));
        List<TopicPartition> partitions = new ArrayList<>();
        for (String topic : topics) {
            partitions.add(new TopicPartition(topic, 0));
        }
        verifier.assign(partitions);
        verifier.seekToBeginning(partitions);
    }

    private void startTask(Sink sink) {
        shuttingDown.set(false);
        taskThread = launchTask(source, sink, "task-kafka-source-integration", taskFailure);
    }

    private Thread launchTask(KafkaSource kafkaSource, Sink sink, String name,
            AtomicReference<Throwable> laneFailure) {
        Task task;
        try {
            task = new Task(name, kafkaSource, new SQLTransform<>(SQL), sink, 1);
        } catch (RuntimeException | Error failure) {
            try {
                kafkaSource.close();
            } finally {
                sink.close();
            }
            throw failure;
        }
        return Thread.ofVirtual().name(name).start(() -> {
            try {
                task.launch();
                if (!shuttingDown.get()) {
                    laneFailure.compareAndSet(null, new IllegalStateException("lane exited before explicit shutdown"));
                }
            } catch (InterruptedException failure) {
                if (!shuttingDown.get()) {
                    laneFailure.compareAndSet(null, failure);
                }
                Thread.currentThread().interrupt();
            } catch (Throwable failure) {
                if (shuttingDown.get() && failure instanceof RuntimeException
                        && failure.getCause() instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                } else {
                    laneFailure.compareAndSet(null, failure);
                }
            }
        });
    }

    private void stopTask() {
        shuttingDown.set(true);
        if (taskThread != null) {
            taskThread.interrupt();
        }
        if (secondTaskThread != null) {
            secondTaskThread.interrupt();
        }
        assertAll("both task lanes and owners must stop",
            () -> {
                if (taskThread != null) {
                    taskThread.join(15_000);
                    assertFalse(taskThread.isAlive(), "first lane and consumer owner must stop within close timeout");
                    taskThread = null;
                }
            },
            () -> {
                if (secondTaskThread != null) {
                    secondTaskThread.join(15_000);
                    assertFalse(secondTaskThread.isAlive(), "second lane and consumer owner must stop within close timeout");
                    secondTaskThread = null;
                }
            },
            this::assertTaskHealthy);
    }

    private GateSink gate() {
        GateSink sink = new GateSink();
        gates.add(sink);
        return sink;
    }

    private static final class GateSink implements Sink {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicReference<Record<String, String>> active = new AtomicReference<>();
        final LinkedBlockingQueue<Record<String, String>> published = new LinkedBlockingQueue<>();

        @Override
        public void publish(Record<String, String> record) {
            active.set(record);
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(failure);
            }
            published.add(record);
        }

        @Override
        public void close() {
            release.countDown();
        }
    }

    private static final class RebalanceGateSink implements Sink {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicReference<Record<String, String>> active = new AtomicReference<>();
        final LinkedBlockingQueue<Record<String, String>> published = new LinkedBlockingQueue<>();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final KafkaSink delegate;
        private final String gatedKey;

        RebalanceGateSink(KafkaSink delegate, String gatedKey) {
            this.delegate = delegate;
            this.gatedKey = gatedKey;
        }

        @Override
        public void publish(Record<String, String> record) {
            if (gatedKey.equals(record.key())) {
                active.set(record);
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(failure);
                }
            }
            delegate.publish(record);
            published.add(record);
        }

        @Override
        public void close() {
            release.countDown();
            if (closed.compareAndSet(false, true)) {
                delegate.close();
            }
        }
    }

    /** Native test assignor fixes the moved partition without relying on generated consumer member IDs. */
    public static final class DeterministicRebalanceAssignor implements ConsumerPartitionAssignor, Configurable {
        private String clientId;

        @Override
        public void configure(Map<String, ?> configs) {
            clientId = (String) configs.get(ConsumerConfig.CLIENT_ID_CONFIG);
        }

        @Override
        public ByteBuffer subscriptionUserData(Set<String> topics) {
            return ByteBuffer.wrap(utf8(clientId));
        }

        @Override
        public GroupAssignment assign(Cluster metadata, GroupSubscription subscription) {
            List<Map.Entry<String, Subscription>> members = new ArrayList<>(subscription.groupSubscription().entrySet());
            members.sort(Comparator.comparing(entry ->
                StandardCharsets.UTF_8.decode(entry.getValue().userData().duplicate()).toString()));
            String topic = members.getFirst().getValue().topics().getFirst();
            Map<String, Assignment> assignments = new HashMap<>();
            for (int member = 0; member < members.size(); member++) {
                List<TopicPartition> partitions = new ArrayList<>();
                for (int partition = member; partition < metadata.partitionCountForTopic(topic); partition += members.size()) {
                    partitions.add(new TopicPartition(topic, partition));
                }
                assignments.put(members.get(member).getKey(), new Assignment(partitions));
            }
            return new GroupAssignment(assignments);
        }

        @Override
        public String name() {
            return "deterministic-rebalance-integration";
        }
    }

    private Record<String, String> receivePublished(GateSink sink) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            assertTaskHealthy();
            Record<String, String> record = sink.published.poll(250, TimeUnit.MILLISECONDS);
            if (record != null) {
                return record;
            }
        }
        fail("expected a completed SQL result from the sink");
        return null;
    }

    private ConsumerGroupDescription describeGroup(String group) throws Exception {
        return admin.describeConsumerGroups(List.of(group)).describedGroups().get(group)
            .get(10, TimeUnit.SECONDS);
    }

    private MemberDescription awaitAssigned(String group, TopicPartition partition) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            assertTaskHealthy();
            ConsumerGroupDescription description = describeGroup(group);
            if (description.members().size() == 1) {
                MemberDescription member = description.members().iterator().next();
                if (member.assignment().topicPartitions().equals(Set.of(partition))) {
                    return member;
                }
            }
            Thread.sleep(100);
        }
        fail("consumer did not obtain assignment for " + partition);
        return null;
    }

    private MemberDescription awaitMemberAssignment(String group, String clientId,
            Set<TopicPartition> expected, int memberCount) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            assertTaskHealthy();
            ConsumerGroupDescription description;
            try {
                description = describeGroup(group);
            } catch (ExecutionException notYetCreated) {
                if (!(notYetCreated.getCause() instanceof GroupIdNotFoundException)) {
                    throw notYetCreated;
                }
                Thread.sleep(100);
                continue;
            }
            if (description.members().size() == memberCount) {
                for (MemberDescription member : description.members()) {
                    if (member.clientId().equals(clientId) && member.assignment().topicPartitions().equals(expected)) {
                        return member;
                    }
                }
            }
            Thread.sleep(100);
        }
        fail("consumer " + clientId + " did not obtain the deterministic assignment " + expected);
        return null;
    }

    private long committed(String group, TopicPartition partition) throws Exception {
        OffsetAndMetadata offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
            .get(10, TimeUnit.SECONDS).get(partition);
        return offset == null ? -1 : offset.offset();
    }

    private void awaitCommitted(String group, TopicPartition partition, long expected) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        long actual = -1;
        while (System.nanoTime() < deadline) {
            assertTaskHealthy();
            actual = committed(group, partition);
            assertTrue(actual <= expected, "source must never commit past the completed prefix");
            if (actual == expected) {
                return;
            }
            Thread.sleep(100);
        }
        assertEquals(expected, actual, "broker must persist the completed next offset");
    }

    private void assertNotCommittedPast(String group, TopicPartition partition, long unfinishedOffset,
            Duration observation) throws Exception {
        long deadline = System.nanoTime() + observation.toNanos();
        do {
            assertTaskHealthy();
            assertTrue(committed(group, partition) <= unfinishedOffset,
                "broker offset must not pass the active unfinished record or its buffered followers");
            Thread.sleep(100);
        } while (System.nanoTime() < deadline);
    }

    private List<ConsumerRecord<String, String>> receiveRecords(int expected) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (records.size() < expected && System.nanoTime() < deadline) {
            assertTaskHealthy();
            verifier.poll(Duration.ofMillis(250)).forEach(records::add);
        }
        assertEquals(expected, records.size(), "only matching, non-tombstone, non-loop inputs should be published");
        return records;
    }

    private void assertNoMoreRecords() {
        long deadline = System.nanoTime() + NEGATIVE_WINDOW.toNanos();
        while (System.nanoTime() < deadline) {
            assertTaskHealthy();
            assertTrue(verifier.poll(Duration.ofMillis(250)).isEmpty(), "no filtered/tombstone/loop/routing copies");
        }
    }

    private void assertTaskHealthy() {
        if (taskFailure.get() != null) {
            fail("first task lane terminated unexpectedly", taskFailure.get());
        }
        if (secondTaskFailure.get() != null) {
            fail("second task lane terminated unexpectedly", secondTaskFailure.get());
        }
    }

    private static ConsumerRecord<String, String> onTopic(List<ConsumerRecord<String, String>> records,
            String topic) {
        List<ConsumerRecord<String, String>> matching = records.stream()
            .filter(record -> record.topic().equals(topic)).toList();
        assertEquals(1, matching.size(), "expected exactly one SQL result on " + topic);
        return matching.getFirst();
    }

    private static void assertOutput(ConsumerRecord<String, String> record, String device, int doubled,
            String key, long timestamp, String inputTopic, RecordMetadata input) throws Exception {
        assertEquals(MAPPER.valueToTree(Map.of("device", device, "doubled", doubled)), MAPPER.readTree(record.value()));
        assertEquals(key, record.key());
        assertEquals(timestamp, record.timestamp());
        assertEquals(List.of(inputTopic), headerValues(record, "src.kafka.topic"));
        assertEquals(List.of(Integer.toString(input.partition())), headerValues(record, "kafka.partition"));
        assertEquals(List.of(Long.toString(input.offset())), headerValues(record, "kafka.offset"));
        assertEquals("1", header(record, Task.HOP_COUNT_HEADER));
    }

    private static String header(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        assertNotNull(header, "missing header " + key);
        assertNotNull(header.value(), "null header " + key);
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static List<String> headerValues(ConsumerRecord<String, String> record, String key) {
        List<String> values = new ArrayList<>();
        for (Header header : record.headers().headers(key)) {
            values.add(new String(header.value(), StandardCharsets.UTF_8));
        }
        return values;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @AfterEach
    void tearDown() {
        assertAll("stop task and close every client even if a cleanup fails",
            this::stopTask,
            () -> {
                for (GateSink sink : gates) {
                    sink.release.countDown();
                }
                for (RebalanceGateSink sink : rebalanceGates) {
                    sink.release.countDown();
                }
                if (source != null) {
                    source.close();
                }
            },
            () -> {
                if (secondSource != null) {
                    secondSource.close();
                }
            },
            () -> {
                assertAll(rebalanceGates.stream().map(sink -> (Executable) sink::close));
            },
            () -> {
                if (publisher != null) {
                    publisher.close(Duration.ofSeconds(10));
                }
            },
            () -> {
                if (verifier != null) {
                    verifier.close(Duration.ofSeconds(10));
                }
            },
            () -> {
                if (admin != null) {
                    admin.close(Duration.ofSeconds(10));
                }
            },
            this::assertTaskHealthy);
    }
}
