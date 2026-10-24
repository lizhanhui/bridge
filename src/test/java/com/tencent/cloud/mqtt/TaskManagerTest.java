package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.cloud.mqtt.model.Connector;
import com.tencent.cloud.mqtt.model.ConnectorType;

class TaskManagerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode tasksNode(String tasksJson) throws Exception {
        return MAPPER.readTree("{\"tasks\": " + tasksJson + "}").required("tasks");
    }

    private static String taskJson(String name, String parallelism) {
        String p = parallelism == null ? "" : "\"parallelism\": " + parallelism + ", ";
        return "{\"name\": \"" + name + "\", " + p
            + "\"source\": {\"connector_id\": \"c\", \"topic_filter\": \"$share/g/t/#\"}, "
            + "\"sql\": \"SELECT * FROM payload\", "
            + "\"sink\": {\"connector_id\": \"c\", \"topic\": \"out\"}}";
    }

    @Test
    void kafkaConnectorNeedsNativeBootstrapButNoAccessPoint() throws Exception {
        JsonNode node = MAPPER.readTree("""
            [{"id":"k","type":"Kafka","username":"ignored","password":"ignored",
              "properties":{"bootstrap.servers":"localhost:9092","linger.ms":12,
                            "enable.idempotence":true,"ssl.enabled.protocols":["TLSv1.3"]}}]
            """);
        Connector connector = TaskManager.parseConnectors(node).get("k");
        assertEquals(ConnectorType.Kafka, connector.getType());
        assertEquals(null, connector.getAccessPoint());
        assertEquals("localhost:9092", connector.getProperties().get("bootstrap.servers"));
        assertEquals(12, connector.getProperties().get("linger.ms"));
        assertEquals(true, connector.getProperties().get("enable.idempotence"));
        assertEquals(List.of("TLSv1.3"), connector.getProperties().get("ssl.enabled.protocols"));
    }

    @Test
    void kafkaBootstrapSupportsNativeStringLists() throws Exception {
        JsonNode node = MAPPER.readTree("""
            [{"id":"k","type":"Kafka","properties":{"bootstrap.servers":["localhost:9092","localhost:9093"]}}]
            """);
        assertEquals(List.of("localhost:9092", "localhost:9093"),
            TaskManager.parseConnectors(node).get("k").getProperties().get("bootstrap.servers"));
    }

    @Test
    void kafkaRejectsMissingBlankAndWrongTypeProperties() throws Exception {
        for (String properties : List.of("", ",\"properties\":null", ",\"properties\":[]",
                ",\"properties\":\"bad\"", ",\"properties\":{}",
                ",\"properties\":{\"bootstrap.servers\":\" \"}",
                ",\"properties\":{\"bootstrap.servers\":[]}",
                ",\"properties\":{\"bootstrap.servers\":[\" \" ]}",
                ",\"properties\":{\"bootstrap.servers\":9092}")) {
            JsonNode node = MAPPER.readTree("[{\"id\":\"k\",\"type\":\"Kafka\"" + properties + "}]");
            assertThrows(IllegalArgumentException.class, () -> TaskManager.parseConnectors(node));
        }
    }

    @Test
    void connectorPropertiesDefaultToEmptyAndExistingTypesRetainSchema() throws Exception {
        assertTrue(new Connector().getProperties().isEmpty());
        JsonNode node = MAPPER.readTree("""
            [{"id":"m","type":"MQTT","access_point":"localhost:1883","username":"u","password":"p"},
             {"id":"r","type":"RocketMQ","access_point":"localhost:8081"}]
            """);
        Map<String, Connector> connectors = TaskManager.parseConnectors(node);
        assertEquals("localhost:1883", connectors.get("m").getAccessPoint());
        assertEquals("u", connectors.get("m").getUsername());
        assertEquals("p", connectors.get("m").getPassword());
        assertTrue(connectors.get("m").getProperties().isEmpty());
        for (String type : List.of("MQTT", "RocketMQ")) {
            JsonNode invalid = MAPPER.readTree("[{\"id\":\"c\",\"type\":\"" + type + "\"}]");
            assertThrows(IllegalArgumentException.class, () -> TaskManager.parseConnectors(invalid));
        }
    }

    @Test
    void mqttIgnoresPropertiesFieldAsBefore() throws Exception {
        assertIgnoresPropertiesField("MQTT");
    }

    @Test
    void rocketMqIgnoresPropertiesFieldAsBefore() throws Exception {
        assertIgnoresPropertiesField("RocketMQ");
    }

    private static void assertIgnoresPropertiesField(String type) throws Exception {
        for (String properties : List.of("null", "[]", "\"ignored\"", "{\"ignored\":true}")) {
            JsonNode node = MAPPER.readTree("[{\"id\":\"c\",\"type\":\"" + type
                + "\",\"access_point\":\"localhost:1234\",\"properties\":" + properties + "}]");
            Connector connector = TaskManager.parseConnectors(node).get("c");
            assertEquals(ConnectorType.valueOf(type), connector.getType());
            assertEquals("localhost:1234", connector.getAccessPoint());
            assertTrue(connector.getProperties().isEmpty(), "existing types must keep ignoring properties");
        }
    }

    @Test
    void kafkaSourceIsRejectedExplicitlyWithoutBuildingClients() throws Exception {
        Connector connector = new Connector();
        connector.setType(ConnectorType.Kafka);
        JsonNode source = MAPPER.readTree("{\"connector_id\":\"k\"}");
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> TaskManager.parseSource(source, Map.of("k", connector), "task-0"));
        assertTrue(failure.getMessage().contains("Kafka"));
        assertTrue(failure.getMessage().toLowerCase().contains("source"));
        assertTrue(failure.getMessage().toLowerCase().contains("not supported"));
    }

    @Test
    void defaultsToSingleSuffixedLane() throws Exception {
        List<TaskManager.TaskSpec> specs =
            TaskManager.expandTaskConfigs(tasksNode("[" + taskJson("t", null) + "]"));
        assertEquals(1, specs.size());
        assertEquals("t-0", specs.get(0).name());
        assertEquals("SELECT * FROM payload", specs.get(0).sql());
        assertEquals(1, specs.get(0).maxHops());
    }

    @Test
    void expandsParallelismIntoNumberedLanes() throws Exception {
        List<TaskManager.TaskSpec> specs =
            TaskManager.expandTaskConfigs(tasksNode("[" + taskJson("t", "3") + "]"));
        assertEquals(3, specs.size());
        assertEquals("t-0", specs.get(0).name());
        assertEquals("t-1", specs.get(1).name());
        assertEquals("t-2", specs.get(2).name());
    }

    @Test
    void expandsMultipleTasksIndependently() throws Exception {
        List<TaskManager.TaskSpec> specs = TaskManager.expandTaskConfigs(
            tasksNode("[" + taskJson("a", "2") + ", " + taskJson("b", null) + "]"));
        assertEquals(List.of("a-0", "a-1", "b-0"),
            specs.stream().map(TaskManager.TaskSpec::name).toList());
    }

    @Test
    void rejectsParallelismBelowOne() throws Exception {
        for (String bad : new String[] {"0", "-2"}) {
            JsonNode node = tasksNode("[" + taskJson("t", bad) + "]");
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> TaskManager.expandTaskConfigs(node));
            assertTrue(e.getMessage().contains("t"),
                "message should name the offending task: " + e.getMessage());
        }
    }

    @Test
    void rejectsDuplicateLaneNames() throws Exception {
        // Two tasks with the same base name both produce lane "t-0".
        JsonNode node = tasksNode(
            "[" + taskJson("t", "2") + ", " + taskJson("t", null) + "]");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> TaskManager.expandTaskConfigs(node));
        assertTrue(e.getMessage().contains("t-0"),
            "message should name the duplicate lane: " + e.getMessage());
    }

    @Test
    void preservesMaxHopsAcrossLanes() throws Exception {
        JsonNode node = tasksNode(
            "[{\"name\": \"t\", \"parallelism\": 2, \"max_hops\": 5, "
            + "\"source\": {\"connector_id\": \"c\", \"topic_filter\": \"t/#\"}, "
            + "\"sql\": \"SELECT * FROM payload\", "
            + "\"sink\": {\"connector_id\": \"c\", \"topic\": \"out\"}}]");
        List<TaskManager.TaskSpec> specs = TaskManager.expandTaskConfigs(node);
        assertEquals(5, specs.get(0).maxHops());
        assertEquals(5, specs.get(1).maxHops());
    }

    @Test
    void specCarriesParallelismForLoadTimeChecks() throws Exception {
        List<TaskManager.TaskSpec> specs =
            TaskManager.expandTaskConfigs(tasksNode("[" + taskJson("t", "3") + "]"));
        assertEquals(3, specs.get(0).parallelism());
    }
}
