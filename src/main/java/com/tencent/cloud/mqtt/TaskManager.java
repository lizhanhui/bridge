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

    /** Default loop-prevention limit when a task does not set {@code max_hops}. */
    private static final int DEFAULT_MAX_HOPS = 1;

    /** Default parallelism when a task does not set {@code parallelism}. */
    private static final int DEFAULT_PARALLELISM = 1;

    /** One lane of a task: a fully-named task ready for connector construction. */
    record TaskSpec(String name, String sql, int maxHops, int parallelism,
                    JsonNode source, JsonNode sink) {}

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

    /**
     * Expands each task config into {@code parallelism} lane specs named
     * {@code <name>-<seq>}. Pure: no connectors are resolved and no clients
     * are built, so it is unit-testable without brokers.
     */
    static List<TaskSpec> expandTaskConfigs(JsonNode tasksNode) {
        List<TaskSpec> specs = new ArrayList<>();
        for (JsonNode taskNode : tasksNode) {
            String name = taskNode.required("name").asText();
            int parallelism = taskNode.path("parallelism").asInt(DEFAULT_PARALLELISM);
            if (parallelism < 1) {
                throw new IllegalArgumentException(
                    "Task " + name + ": parallelism must be >= 1, got " + parallelism);
            }
            String sql = taskNode.required("sql").asText();
            int maxHops = taskNode.path("max_hops").asInt(DEFAULT_MAX_HOPS);
            JsonNode source = taskNode.required("source");
            JsonNode sink = taskNode.required("sink");
            for (int seq = 0; seq < parallelism; seq++) {
                specs.add(new TaskSpec(name + "-" + seq, sql, maxHops, parallelism, source, sink));
            }
        }
        return specs;
    }

    static List<Task> loadTasks(Path configPath) throws IOException {
        JsonNode root = new ObjectMapper().readTree(configPath.toFile());
        Map<String, Connector> connectors = parseConnectors(root.required("connectors"));

        List<Task> tasks = new ArrayList<>();
        for (TaskSpec spec : expandTaskConfigs(root.required("tasks"))) {
            warnIfUnsharedMqttSource(spec, connectors);
            Source source = null;
            Sink sink = null;
            try {
                source = parseSource(spec.source(), connectors, spec.name());
                sink = parseSink(spec.sink(), connectors, spec.name());
            } catch (RuntimeException e) {
                // Don't leak clients from a partially-built lane
                if (source != null) {
                    source.close();
                }
                if (sink != null) {
                    sink.close();
                }
                throw e;
            }
            Transform<String, String> transform = new SQLTransform<>(spec.sql());
            tasks.add(new Task(spec.name(), source, transform, sink, spec.maxHops()));
        }
        return tasks;
    }

    /**
     * Parallel MQTT lanes only split load under a shared subscription
     * ($share/<group>/...); with a plain filter every lane receives every
     * message, duplicating deliveries at the sink.
     */
    private static void warnIfUnsharedMqttSource(TaskSpec spec, Map<String, Connector> connectors) {
        if (spec.parallelism() <= 1) {
            return;
        }
        Connector connector = connectors.get(spec.source().path("connector_id").asText());
        if (connector == null || connector.getType() != ConnectorType.MQTT) {
            return;
        }
        String topicFilter = spec.source().path("topic_filter").asText("");
        if (!topicFilter.startsWith("$share/")) {
            log.warn("Task {}: parallelism > 1 with non-shared MQTT filter '{}' "
                + "— every lane will receive every message (N× duplicates at sink)",
                spec.name(), topicFilter);
        }
    }

    private static Map<String, Connector> parseConnectors(JsonNode connectorsNode) {
        Map<String, Connector> connectors = new HashMap<>();
        for (JsonNode node : connectorsNode) {
            Connector connector = new Connector();
            connector.setId(node.required("id").asText());
            connector.setType(ConnectorType.valueOf(node.required("type").asText()));
            connector.setAccessPoint(node.required("access_point").asText());
            connector.setUsername(node.path("username").asText(null));
            connector.setPassword(node.path("password").asText(null));
            connectors.put(connector.getId(), connector);
        }
        return connectors;
    }

    private static Source parseSource(JsonNode node, Map<String, Connector> connectors, String taskName) {
        Connector connector = resolveConnector(node, connectors);
        return switch (connector.getType()) {
            case MQTT -> new MqttSource(connector, node.required("topic_filter").asText(),
                "source-" + taskName);
            case RocketMQ -> throw new UnsupportedOperationException("RocketMQ source not yet implemented");
        };
    }

    private static Sink parseSink(JsonNode node, Map<String, Connector> connectors, String taskName) {
        Connector connector = resolveConnector(node, connectors);
        return switch (connector.getType()) {
            case MQTT -> new MqttSink(connector, node.required("topic").asText(),
                "sink-" + taskName);
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
