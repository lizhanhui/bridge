# Task Parallelism Design

Date: 2026-08-15
Status: Approved

## Problem

Each task currently runs a single processing pipeline (one source, one
transform, one sink on one virtual thread). To serve heavier workloads, a
task needs configurable parallelism.

## Model: Independent Lanes

A task config entry with `parallelism: N` expands into **N independent lane
`Task` instances**, named `<name>-0` … `<name>-N-1`. Each lane gets its own
freshly-built source, transform, and sink:

- No state is shared across lanes — no thread-safety concerns (PartiQL
  `Expression.eval` is not documented as thread-safe, so each lane gets its
  own `SQLTransform` instance with its own compiled expression).
- Each MQTT lane gets its own client. Client IDs become
  `bridge-<connectorId>-source-<taskName>-<seq>` (and `sink-` likewise),
  i.e. task name + parallelism sequence, with zero changes to
  `MqttClients`/`MqttSource`/`MqttSink` — lane names flow through the
  existing `"source-" + taskName` suffix logic.

`Task.java` is untouched: its `launch()` loop, hop-count logic, and close
semantics are already per-lane. `TaskManager.main` starts one virtual thread
per lane; the existing shutdown hook interrupts all of them.

## MQTT Shared-Subscription Caveat

Parallel source lanes only split load correctly when the topic filter is a
shared subscription (`$share/<group>/...`): all lanes use the identical
filter, so they land in the same group and the broker load-balances among
them. With a plain filter, every lane receives every message → N× duplicate
delivery at the sink.

`TaskManager` logs a loud warning at load time when `parallelism > 1` and an
MQTT source filter does not start with `$share/`, but does not reject it
(valid for intentional fan-out, and for future non-MQTT sources).

## Config Schema

Each task in `conf/tasks.json` gains an optional integer field:

```json
{
  "name": "mqtt2rmq-task-name",
  "parallelism": 4,
  "source": { "connector_id": "...", "topic_filter": "$share/g/home/#" },
  "sql": "SELECT ... FROM payload WHERE ...",
  "sink": { "connector_id": "...", "topic": "home" }
}
```

- `parallelism` is read via `taskNode.path("parallelism").asInt(1)` — default 1.
- Values `< 1` are rejected with `IllegalArgumentException` naming the task
  (fail fast at startup, same style as unknown `connector_id`).
- Lanes are **always** suffixed `-<seq>`, even when `parallelism` is 1.
  (Decision: uniformity over preserving existing client IDs.)

## Lane Expansion in TaskManager

After parsing `name`/`sql`/`max_hops`, `loadTasks` loops `seq` from
`0` to `parallelism - 1` and builds one `Task` per seq named `<name>-<seq>`.
`parseSource`/`parseSink` are called per lane (they already take `taskName`),
so each lane constructs its own clients. Thread naming, logging, and the
shutdown hook work unchanged — lanes are ordinary `Task`s in the list.

## Error Handling

- **Startup:** if lane construction fails partway (e.g. lane 2's MQTT
  connect throws), any sources/sinks already built for that task are closed
  in a catch block before rethrowing, so a bad config doesn't leak
  connections.
- **Runtime:** a lane dying with an error logs and stops that lane only —
  same per-task isolation as today, just more lanes.

## Testing

Client construction happens in the `MqttSource`/`MqttSink` constructors, so
`loadTasks` can't be unit-tested without a broker. To keep this testable:

- Extract a pure `expandTaskConfigs(JsonNode) → List<TaskSpec>` step
  (name/sql/maxHops/connector refs per lane, no client construction).
- Unit-test that step: a `parallelism: 3` task yields 3 specs named
  `name-0/1/2`; `parallelism: 0` is rejected; absent field defaults to 1.
- Client construction stays in `loadTasks`, consuming the specs.
