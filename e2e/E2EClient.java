import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;

/**
 * End-to-end verification for the bridge, run against the live clusters in
 * conf/tasks.json with the bridge (TaskManager) already running.
 *
 * Compile: javac -cp target/bridge-1.0-SNAPSHOT.jar -d e2e/classes e2e/E2EClient.java
 * Run:     java  -cp target/bridge-1.0-SNAPSHOT.jar:e2e/classes E2EClient
 *
 * Test 1 (MQTT -> RMQ): publishes a filter-matching message A and a
 * non-matching message B to home/e2e/*, then asserts via a SimpleConsumer in
 * the dedicated group "e2e-verify" on RocketMQ topic "home" that A arrived
 * both as full payload (filter task) and as a projection row (projection
 * task), and that B did not arrive.
 *
 * Test 2 (RMQ -> MQTT): subscribes MQTT home/room/1, publishes a matching
 * message C and a non-matching message D to RocketMQ topic "iov", asserts
 * only C is bridged by the rmq2mqtt task.
 *
 * NOTE: these clusters are shared — consumer group Group-0 and the MQTT
 * topics see foreign traffic. Assertions are therefore made on the dedicated
 * e2e-verify group (T1) and on presence/absence of unique run markers.
 */
public class E2EClient {
    private static final Duration WAIT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofSeconds(3);

    public static void main(String[] args) throws Exception {
        JsonNode root = new ObjectMapper().readTree(new File("conf/tasks.json"));
        JsonNode mqttConn = null, rmqConn = null;
        for (JsonNode c : root.get("connectors")) {
            if ("MQTT".equals(c.get("type").asText())) mqttConn = c;
            if ("RocketMQ".equals(c.get("type").asText())) rmqConn = c;
        }
        String mqttAccess = mqttConn.get("access_point").asText();
        String mqttHost = mqttAccess.substring(0, mqttAccess.lastIndexOf(':'));
        int mqttPort = Integer.parseInt(mqttAccess.substring(mqttAccess.lastIndexOf(':') + 1));

        String runId = UUID.randomUUID().toString().substring(0, 8);
        System.out.println("E2E run id: " + runId);

        Mqtt5AsyncClient mqtt = MqttClient.builder().useMqttVersion5()
            .identifier("e2e-client-" + runId)
            .serverHost(mqttHost).serverPort(mqttPort)
            .sslWithDefaultConfig()
            .simpleAuth()
                .username(mqttConn.get("username").asText())
                .password(mqttConn.get("password").asText().getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
            .buildAsync();
        mqtt.connectWith().cleanStart(true).send().join();
        System.out.println("MQTT connected: " + mqttAccess);

        ConcurrentLinkedQueue<String> received = new ConcurrentLinkedQueue<>();
        mqtt.subscribeWith().topicFilter("home/room/1")
            .callback(pub -> {
                String body = new String(pub.getPayloadAsBytes(), StandardCharsets.UTF_8);
                received.add(body);
                System.out.println("MQTT home/room/1 <- " + body);
            })
            .send().join();
        // Debug-only tap on the whole tree; not used for assertions.
        mqtt.subscribeWith().topicFilter("home/#")
            .callback(pub -> System.out.println("MQTT tap " + pub.getTopic() + " <- "
                + new String(pub.getPayloadAsBytes(), StandardCharsets.UTF_8)))
            .send().join();
        System.out.println("MQTT subscribed: home/room/1 (+ home/# debug tap)");

        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration rmqConf = ClientConfiguration.newBuilder()
            .setEndpoints(rmqConn.get("access_point").asText())
            .setCredentialProvider(new StaticSessionCredentialsProvider(
                rmqConn.get("username").asText(), rmqConn.get("password").asText()))
            .build();
        SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
            .setClientConfiguration(rmqConf)
            .setConsumerGroup("e2e-verify")
            .setSubscriptionExpressions(Map.of("home", new FilterExpression()))
            .setAwaitDuration(POLL)
            .build();
        Producer producer = provider.newProducerBuilder()
            .setClientConfiguration(rmqConf)
            .setTopics("iov")
            .build();
        System.out.println("RocketMQ consumer (e2e-verify) and producer ready");

        // ---- Test 1: MQTT -> RocketMQ ----
        String idA = "A-" + runId; // matches filter: age < 30 && address.number == 555
        String idB = "B-" + runId; // filtered out (age 50)
        publishMqtt(mqtt, "home/e2e/match",
            "{\"id\":\"" + idA + "\",\"age\":25,\"address\":{\"number\":555}}");
        publishMqtt(mqtt, "home/e2e/nomatch",
            "{\"id\":\"" + idB + "\",\"age\":50,\"address\":{\"number\":555}}");
        System.out.println("T1 published " + idA + " (match) and " + idB + " (no match) to MQTT home/e2e/*");

        StringBuilder rmqBodies = new StringBuilder();
        boolean sawFull = false, sawProjection = false;
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline && !(sawFull && sawProjection)) {
            List<MessageView> views = consumer.receive(10, Duration.ofSeconds(15));
            for (MessageView v : views) {
                String body = StandardCharsets.UTF_8.decode(v.getBody()).toString();
                rmqBodies.append(body).append('\n');
                consumer.ack(v);
                System.out.println("RMQ home <- " + body);
                if (body.contains(idA)) sawFull = true;
                if (!body.contains("\"id\"") && body.contains("age") && body.contains("555")) sawProjection = true;
            }
        }
        boolean ok = true;
        ok &= check("T1 filter task bridged matching message to RMQ home", sawFull);
        ok &= check("T1 projection task bridged projected row to RMQ home", sawProjection);
        ok &= check("T1 non-matching message was filtered out", !rmqBodies.toString().contains(idB));

        // ---- Test 2: RocketMQ -> MQTT ----
        String idC = "C-" + runId; // matches filter
        String idD = "D-" + runId; // filtered out (age 99)
        sendRmq(producer, "iov", "{\"id\":\"" + idC + "\",\"age\":20,\"address\":{\"number\":555}}");
        sendRmq(producer, "iov", "{\"id\":\"" + idD + "\",\"age\":99,\"address\":{\"number\":555}}");
        System.out.println("T2 published " + idC + " (match) and " + idD + " (no match) to RMQ iov");

        boolean sawC = false;
        deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline && !sawC) {
            Thread.sleep(500);
            for (String body : received) {
                if (body.contains(idC)) sawC = true;
            }
        }
        ok &= check("T2 matching RMQ iov message bridged to MQTT home/room/1", sawC);
        boolean sawD = false;
        for (String body : received) {
            if (body.contains(idD)) sawD = true;
        }
        ok &= check("T2 non-matching RMQ message was filtered out", !sawD);

        producer.close();
        consumer.close();
        mqtt.disconnect().join();

        System.out.println(ok ? "E2E RESULT: PASS" : "E2E RESULT: FAIL");
        System.exit(ok ? 0 : 1);
    }

    private static void publishMqtt(Mqtt5AsyncClient mqtt, String topic, String payload) {
        mqtt.publishWith().topic(topic).payload(payload.getBytes(StandardCharsets.UTF_8)).send().join();
    }

    private static void sendRmq(Producer producer, String topic, String body) throws Exception {
        Message msg = ClientServiceProvider.loadService().newMessageBuilder()
            .setTopic(topic)
            .setBody(body.getBytes(StandardCharsets.UTF_8))
            .build();
        producer.send(msg);
    }

    private static boolean check(String label, boolean passed) {
        System.out.println((passed ? "  PASS: " : "  FAIL: ") + label);
        return passed;
    }
}
