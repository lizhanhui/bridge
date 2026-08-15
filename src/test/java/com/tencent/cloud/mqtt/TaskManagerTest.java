package com.tencent.cloud.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
            assertThrows(IllegalArgumentException.class,
                () -> TaskManager.expandTaskConfigs(node));
        }
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
