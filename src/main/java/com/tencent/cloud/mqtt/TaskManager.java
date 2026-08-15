package com.tencent.cloud.mqtt;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.cloud.mqtt.model.Connector;
import com.tencent.cloud.mqtt.model.ConnectorType;
import com.tencent.cloud.mqtt.mqtt.MqttSink;
import com.tencent.cloud.mqtt.mqtt.MqttSource;

public class TaskManager {
    private static final Logger log = LoggerFactory.getLogger(TaskManager.class);

    private static final String DEFAULT_CONFIG_PATH = "conf/tasks.json";

    public static void main(String[] args) throws IOException, InterruptedException {
        Path configPath = Path.of(args.length > 0 ? args[0] : DEFAULT_CONFIG_PATH);
        List<Task> tasks = loadTasks(configPath);

        List<Thread> threads = new ArrayList<>();
        for (Task task : tasks) {
            Thread thread = Thread.ofVirtual().name("task-" + task.getName()).start(() -> {
                try {
                    task.launch();
                } catch (InterruptedException e) {
                    log.info("Task {} interrupted, stopping", task.getName());
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    log.error("Task {} terminated with an error", task.getName(), t);
                }
            });
            threads.add(thread);
            log.info("Launched task {} on virtual thread {}", task.getName(), thread.getName());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown requested, interrupting task threads");
            threads.forEach(Thread::interrupt);
        }));

        for (Thread thread : threads) {
            thread.join();
        }
    }

    static List<Task> loadTasks(Path configPath) throws IOException {
        JsonNode root = new ObjectMapper().readTree(configPath.toFile());
        Map<String, Connector> connectors = parseConnectors(root.required("connectors"));

        List<Task> tasks = new ArrayList<>();
        for (JsonNode taskNode : root.required("tasks")) {
            String name = taskNode.required("name").asText();
            Source source = parseSource(taskNode.required("source"), connectors);
            Transform<String, String> transform = new SQLTransform<>(taskNode.required("sql").asText());
            Sink sink = parseSink(taskNode.required("sink"), connectors);
            tasks.add(new Task(name, source, transform, sink));
        }
        return tasks;
    }

    private static Map<String, Connector> parseConnectors(JsonNode connectorsNode) {
        Map<String, Connector> connectors = new HashMap<>();
        for (JsonNode node : connectorsNode) {
            Connector connector = new Connector();
            connector.setId(node.required("id").asText());
            connector.setType(ConnectorType.valueOf(node.required("type").asText()));
            connector.setAccessPoint(node.required("access_point").asText());
            connector.setUsername(node.required("username").asText());
            connector.setPassword(node.required("password").asText());
            connectors.put(connector.getId(), connector);
        }
        return connectors;
    }

    private static Source parseSource(JsonNode node, Map<String, Connector> connectors) {
        Connector connector = resolveConnector(node, connectors);
        return switch (connector.getType()) {
            case MQTT -> new MqttSource(connector, node.required("topic_filter").asText());
            case RocketMQ -> throw new UnsupportedOperationException("RocketMQ source not yet implemented");
        };
    }

    private static Sink parseSink(JsonNode node, Map<String, Connector> connectors) {
        Connector connector = resolveConnector(node, connectors);
        return switch (connector.getType()) {
            case MQTT -> new MqttSink(connector, node.required("topic").asText());
            case RocketMQ -> throw new UnsupportedOperationException("RocketMQ sink not yet implemented");
        };
    }

    private static Connector resolveConnector(JsonNode node, Map<String, Connector> connectors) {
        String connectorId = node.required("connector_id").asText();
        Connector connector = connectors.get(connectorId);
        if (connector == null) {
            throw new IllegalArgumentException("Unknown connector_id: " + connectorId);
        }
        return connector;
    }
}
