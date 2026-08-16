package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rocketmq.RocketMQContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.cloud.mqtt.model.Connector;
import com.tencent.cloud.mqtt.model.ConnectorType;
import com.tencent.cloud.mqtt.rocketmq.RocketMQSink;
import com.tencent.cloud.mqtt.rocketmq.RocketMQSource;

@Testcontainers
@Timeout(120)
class RocketMQBridgeIntegrationTest {

    @Container
    static final RocketMQContainer ROCKETMQ = new RocketMQContainer("apache/rocketmq:5.3.4");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration AWAIT_DURATION = Duration.ofSeconds(3);
    private static final Duration INVISIBLE_DURATION = Duration.ofSeconds(30);
    private static final long RECEIVE_TIMEOUT_MILLIS = 20_000;
    private static final long NEGATIVE_WINDOW_MILLIS = 4_000;

    private Thread taskThread;
    private Producer publisher;
    private SimpleConsumer verifier;

    private Connector connector() {
        Connector c = new Connector();
        c.setId("it");
        c.setType(ConnectorType.RocketMQ);
        c.setAccessPoint(ROCKETMQ.getGrpcEndpoints());
        return c;
    }

    /**
     * The 5.x gRPC client eagerly fetches topic routes at producer/consumer
     * startup, so the topic must exist beforehand. The 5.x client has no admin
     * API, so create the topic via mqadmin inside the container and poll until
     * the route is visible (the broker registers topic configs asynchronously;
     * mqadmin exits 0 even on failure).
     */
    private static void createTopic(String topic) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            ROCKETMQ.execInContainer("sh", "-c",
                "sh $ROCKETMQ_HOME/bin/mqadmin updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t "
                    + topic + " >/dev/null 2>&1");
            String route = ROCKETMQ.execInContainer("sh", "-c",
                "sh $ROCKETMQ_HOME/bin/mqadmin topicRoute -n 127.0.0.1:9876 -t " + topic)
                .getStdout();
            if (route.contains("brokerDatas")) {
                return;
            }
            if (System.nanoTime() > deadline) {
                fail("topic route for " + topic + " not registered within 60s");
            }
            Thread.sleep(1_000);
        }
    }

    /** Starts a real Task on a virtual thread; topics must already exist. */
    private void startTask(String name, String inTopic, String sql, String outTopic, int maxHops) {
        RocketMQSource source = new RocketMQSource(connector(), "group-" + name, List.of(inTopic));
        Task task;
        try {
            task = new Task(name,
                source,
                new SQLTransform<>(sql),
                new RocketMQSink(connector(), outTopic),
                maxHops);
        } catch (RuntimeException e) {
            source.close();
            throw e;
        }
        taskThread = Thread.ofVirtual().start(() -> {
            try {
                task.launch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /** Producer for injecting test input; topic must already exist. */
    private Producer publisher(String inTopic) throws ClientException {
        publisher = ClientServiceProvider.loadService().newProducerBuilder()
            .setClientConfiguration(clientConfiguration())
            .setTopics(inTopic)
            .build();
        return publisher;
    }

    /** Verifier consumer subscribed to the given output topics; topics must already exist. */
    private SimpleConsumer subscribeVerifier(String group, String... outTopics) throws ClientException {
        Map<String, FilterExpression> subscriptions = new HashMap<>();
        for (String topic : outTopics) {
            subscriptions.put(topic, FilterExpression.SUB_ALL);
        }
        verifier = ClientServiceProvider.loadService().newSimpleConsumerBuilder()
            .setClientConfiguration(clientConfiguration())
            .setConsumerGroup(group)
            .setSubscriptionExpressions(subscriptions)
            .setAwaitDuration(AWAIT_DURATION)
            .build();
        return verifier;
    }

    private static ClientConfiguration clientConfiguration() {
        return ClientConfiguration.newBuilder()
            .setEndpoints(ROCKETMQ.getGrpcEndpoints())
            .build();
    }

    private void publish(String topic, String payload, String... properties) throws ClientException {
        MessageBuilder builder = ClientServiceProvider.loadService().newMessageBuilder()
            .setTopic(topic)
            .setBody(payload.getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < properties.length; i += 2) {
            builder.addProperty(properties[i], properties[i + 1]);
        }
        publisher.send(builder.build());
    }

    /** Polls until a message arrives or the deadline passes; acks before returning. */
    private static MessageView receive(SimpleConsumer consumer, long timeoutMillis) throws ClientException {
        long deadline = System.nanoTime() + Duration.ofMillis(timeoutMillis).toNanos();
        while (System.nanoTime() < deadline) {
            List<MessageView> messages = consumer.receive(1, INVISIBLE_DURATION);
            if (!messages.isEmpty()) {
                MessageView message = messages.get(0);
                consumer.ack(message);
                return message;
            }
        }
        return null;
    }

    private static String body(MessageView message) {
        ByteBuffer buffer = message.getBody();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (taskThread != null) {
            taskThread.interrupt();
            taskThread.join(10_000);
            assertFalse(taskThread.isAlive(), "task thread survived interrupt");
        }
        if (publisher != null) {
            publisher.close();
        }
        if (verifier != null) {
            verifier.close();
        }
    }

    @Test
    void brokerRoundTrip() throws Exception {
        createTopic("it-smoke");
        Producer producer = publisher("it-smoke");
        SimpleConsumer consumer = subscribeVerifier("group-smoke", "it-smoke");

        producer.send(ClientServiceProvider.loadService().newMessageBuilder()
            .setTopic("it-smoke")
            .setBody("ping".getBytes(StandardCharsets.UTF_8))
            .build());

        MessageView received = receive(consumer, RECEIVE_TIMEOUT_MILLIS);
        assertNotNull(received, "expected message via gRPC round trip");
        assertEquals("ping", body(received));
    }

    @Test
    void transformsAndPublishesMatchingRecord() throws Exception {
        createTopic("it-happy-in");
        createTopic("it-happy-out");
        startTask("happy-0", "it-happy-in",
            "SELECT payload.device AS device, payload.temp * 2 AS doubled FROM payload WHERE payload.temp > 20",
            "it-happy-out", 1);
        SimpleConsumer consumer = subscribeVerifier("group-happy", "it-happy-out");
        publisher("it-happy-in");

        publish("it-happy-in", "{\"device\":\"d1\",\"temp\":25}", "origin", "it");

        MessageView received = receive(consumer, RECEIVE_TIMEOUT_MILLIS);
        assertNotNull(received, "expected transformed message on sink topic");
        assertEquals("it-happy-out", received.getTopic());
        assertEquals(
            MAPPER.readTree("{\"device\":\"d1\",\"doubled\":50}"),
            MAPPER.readTree(body(received)));
        assertEquals("1", received.getProperties().get(Task.HOP_COUNT_HEADER));
        assertEquals("it", received.getProperties().get("origin"));
        assertEquals("it-happy-in", received.getProperties().get("src.rmq.topic"));
    }

    @Test
    void dstTopicHeaderReroutesOutput() throws Exception {
        createTopic("it-dst-in");
        createTopic("it-dst-out-configured");
        createTopic("it-dst-out-override");
        startTask("dst-0", "it-dst-in",
            "SELECT * FROM payload",
            "it-dst-out-configured", 1);
        SimpleConsumer consumer = subscribeVerifier("group-dst",
            "it-dst-out-configured", "it-dst-out-override");
        publisher("it-dst-in");

        publish("it-dst-in", "{\"marker\":\"reroute\"}", "dst.rmq.topic", "it-dst-out-override");

        MessageView received = receive(consumer, RECEIVE_TIMEOUT_MILLIS);
        assertNotNull(received, "expected rerouted message on the dst.rmq.topic topic");
        assertEquals("it-dst-out-override", received.getTopic());
        assertTrue(body(received).contains("reroute"), "unexpected payload: " + body(received));
        assertEquals("it-dst-in", received.getProperties().get("src.rmq.topic"));
        assertNull(received.getProperties().get("dst.rmq.topic"));
        assertNull(receive(consumer, NEGATIVE_WINDOW_MILLIS),
            "nothing should arrive on the sink's configured topic");
    }

    @Test
    void dropsRecordFilteredOutBySql() throws Exception {
        createTopic("it-filter-in");
        createTopic("it-filter-out");
        startTask("filter-0", "it-filter-in",
            "SELECT * FROM payload WHERE payload.temp > 20",
            "it-filter-out", 1);
        SimpleConsumer consumer = subscribeVerifier("group-filter", "it-filter-out");
        publisher("it-filter-in");

        publish("it-filter-in", "{\"device\":\"cold\",\"temp\":5}");
        publish("it-filter-in", "{\"device\":\"hot\",\"temp\":25}");

        MessageView received = receive(consumer, RECEIVE_TIMEOUT_MILLIS);
        assertNotNull(received, "expected the matching message");
        assertTrue(body(received).contains("hot"),
            "only the matching record should be republished, got: " + body(received));
        assertNull(receive(consumer, NEGATIVE_WINDOW_MILLIS),
            "filtered record must not reach the sink");
    }

    @Test
    void dropsRecordExceedingMaxHops() throws Exception {
        createTopic("it-loop-in");
        createTopic("it-loop-out");
        startTask("loop-0", "it-loop-in",
            "SELECT * FROM payload",
            "it-loop-out", 1);
        SimpleConsumer consumer = subscribeVerifier("group-loop", "it-loop-out");
        publisher("it-loop-in");

        publish("it-loop-in", "{\"marker\":\"over-limit\"}", Task.HOP_COUNT_HEADER, "2");
        publish("it-loop-in", "{\"marker\":\"fresh\"}");

        MessageView received = receive(consumer, RECEIVE_TIMEOUT_MILLIS);
        assertNotNull(received, "expected the fresh message");
        assertTrue(body(received).contains("fresh"),
            "over-limit record must be dropped, got: " + body(received));
        assertEquals("1", received.getProperties().get(Task.HOP_COUNT_HEADER));
        assertNull(receive(consumer, NEGATIVE_WINDOW_MILLIS),
            "over-limit record must not be republished");
    }

    @Test
    void dropsMalformedJsonAndKeepsProcessing() throws Exception {
        createTopic("it-badjson-in");
        createTopic("it-badjson-out");
        startTask("badjson-0", "it-badjson-in",
            "SELECT * FROM payload WHERE payload.temp > 20",
            "it-badjson-out", 1);
        SimpleConsumer consumer = subscribeVerifier("group-badjson", "it-badjson-out");
        publisher("it-badjson-in");

        publish("it-badjson-in", "this is not json");
        publish("it-badjson-in", "{\"device\":\"d2\",\"temp\":30}");

        MessageView received = receive(consumer, RECEIVE_TIMEOUT_MILLIS);
        assertNotNull(received, "task lane must survive a malformed record");
        assertTrue(body(received).contains("d2"),
            "expected the valid record after the malformed one, got: " + body(received));
        assertNull(receive(consumer, NEGATIVE_WINDOW_MILLIS),
            "malformed record must not reach the sink");
    }
}
