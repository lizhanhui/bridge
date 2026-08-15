# Task Parallelism Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let a task declare `parallelism: N` in `conf/tasks.json` and run as N independent lane `Task` instances named `<name>-<seq>`, each with its own source/transform/sink and MQTT client.

**Architecture:** Pure expansion step (`expandTaskConfigs`) turns each task config into N `TaskSpec`s (no client construction, unit-testable). `loadTasks` consumes specs and builds one `Task` per lane. Design: `docs/plans/2026-08-15-task-parallelism-design.md`.

**Tech Stack:** Java 25, Maven, JUnit 5, Jackson.

**Worktree:** `/data/repo/bridge/.worktrees/task-parallelism` (branch `feature/task-parallelism`). Run all commands there.

---

### Task 1: `TaskSpec` + `expandTaskConfigs` (pure expansion, TDD)

**Files:**
- Modify: `src/main/java/com/tencent/cloud/mqtt/TaskManager.java`
- Test: `src/test/java/com/tencent/cloud/mqtt/TaskManagerTest.java` (create)

**Step 1: Write the failing test**

Create `src/test/java/com/tencent/cloud/mqtt/TaskManagerTest.java`:

```java
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
}
```

**Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=TaskManagerTest`
Expected: compilation failure — `TaskManager.TaskSpec` / `expandTaskConfigs` do not exist.

**Step 3: Implement expansion in TaskManager**

In `TaskManager.java`, add the nested record and the pure expansion method, and rewrite `loadTasks` to use it (full new `loadTasks`; `parseConnectors`/`parseSource`/`parseSink`/`resolveConnector` unchanged):

```java
    /** Default parallelism when a task does not set {@code parallelism}. */
    private static final int DEFAULT_PARALLELISM = 1;

    /** One lane of a task: a fully-named task ready for connector construction. */
    record TaskSpec(String name, String sql, int maxHops, JsonNode source, JsonNode sink) {}

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
                specs.add(new TaskSpec(name + "-" + seq, sql, maxHops, source, sink));
            }
        }
        return specs;
    }

    static List<Task> loadTasks(Path configPath) throws IOException {
        JsonNode root = new ObjectMapper().readTree(configPath.toFile());
        Map<String, Connector> connectors = parseConnectors(root.required("connectors"));

        List<Task> tasks = new ArrayList<>();
        for (TaskSpec spec : expandTaskConfigs(root.required("tasks"))) {
            Source source = parseSource(spec.source(), connectors, spec.name());
            Transform<String, String> transform = new SQLTransform<>(spec.sql());
            Sink sink = parseSink(spec.sink(), connectors, spec.name());
            tasks.add(new Task(spec.name(), source, transform, sink, spec.maxHops()));
        }
        return tasks;
    }
```

**Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=TaskManagerTest`
Expected: PASS (5 tests)

**Step 5: Commit**

```bash
git add src/main/java/com/tencent/cloud/mqtt/TaskManager.java src/test/java/com/tencent/cloud/mqtt/TaskManagerTest.java
git commit -m "Add parallelism expansion of task configs into numbered lane specs"
```

---

### Task 2: Startup cleanup + shared-subscription warning

**Files:**
- Modify: `src/main/java/com/tencent/cloud/mqtt/TaskManager.java` (`loadTasks`, `parseSource`)

**Step 1: Close partially-built lanes on construction failure**

In `loadTasks`, wrap per-lane construction so already-built `Source`/`Sink` instances for that lane are closed if construction throws:

```java
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
                if (source != null) source.close();
                if (sink != null) sink.close();
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
        Connector connector = connectors.get(spec.source().path("connector_id").asText());
        if (connector == null || connector.getType() != ConnectorType.MQTT) {
            return;
        }
        String topicFilter = spec.source().path("topic_filter").asText("");
        if (!topicFilter.startsWith("$share/") && !isSingleLane(spec)) {
            log.warn("Task {}: parallelism > 1 with non-shared MQTT filter '{}' "
                + "— every lane will receive every message (N× duplicates at sink)",
                spec.name(), topicFilter);
        }
    }
```

`isSingleLane` check: lane names end in `-<seq>`; simpler — check `spec.name().endsWith("-0")` is wrong (parallelism 1 also yields `-0`). Instead pass a boolean: change `warnIfUnsharedMqttSource(TaskSpec spec, ...)` signature to also take `int laneCount`. Compute per task group in `loadTasks` — but specs are a flat list. Simplest correct approach: have `expandTaskConfigs` remain as-is, and in `loadTasks` derive lane count by counting specs sharing the base name. Cleaner alternative: make the warning part of expansion-time knowledge — add `parallelism` to `TaskSpec`:

```java
record TaskSpec(String name, String sql, int maxHops, int parallelism, JsonNode source, JsonNode sink) {}
```

(update the constructor call in `expandTaskConfigs` to pass `parallelism`, and update Task 1 tests' accessor usage accordingly — `maxHops()`/`sql()`/`name()` unchanged, no test breakage). Then:

```java
        if (spec.parallelism() > 1 && !topicFilter.startsWith("$share/")) {
            log.warn(...);
        }
```

**Step 2: Add a test for the parallelism field on the spec**

Append to `TaskManagerTest`:

```java
    @Test
    void specCarriesParallelismForLoadTimeChecks() throws Exception {
        List<TaskManager.TaskSpec> specs =
            TaskManager.expandTaskConfigs(tasksNode("[" + taskJson("t", "3") + "]"));
        assertEquals(3, specs.get(0).parallelism());
    }
```

Run: `mvn test -Dtest=TaskManagerTest` — expect PASS (6 tests).

(The warning itself is log-only and needs a broker to construct `MqttSource`; verified by inspection, not a unit test.)

**Step 3: Full test run**

Run: `mvn test`
Expected: all tests pass.

**Step 4: Commit**

```bash
git add src/main/java/com/tencent/cloud/mqtt/TaskManager.java src/test/java/com/tencent/cloud/mqtt/TaskManagerTest.java
git commit -m "Close partially-built lanes on startup failure and warn on non-shared MQTT filters"
```

---

### Task 3: Config example + docs

**Files:**
- Modify: `conf/tasks.json`
- Modify: `CODEBUDDY.md`

**Step 1: Add `parallelism` to the example config**

In `conf/tasks.json`, add `"parallelism": 2,` to the `mqtt2rmq-task-name` task (after `"name"`).

**Step 2: Update CODEBUDDY.md**

In the `conf/tasks.json` schema paragraph, extend the sentence to note `parallelism` (optional int ≥ 1, default 1; task runs as N independent lanes named `<name>-<seq>`, each with its own clients; MQTT source lanes need a `$share/` filter to split load).

**Step 3: Verify build**

Run: `mvn compile && mvn test`
Expected: success.

**Step 4: Commit**

```bash
git add conf/tasks.json CODEBUDDY.md
git commit -m "Document and exemplify per-task parallelism"
```
