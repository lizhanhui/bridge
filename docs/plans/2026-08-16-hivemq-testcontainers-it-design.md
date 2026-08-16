# HiveMQ Testcontainers Integration Test Design

Date: 2026-08-16
Status: Approved

## Problem

The bridge's MQTT path (`MqttSource` → `SQLTransform` → `MqttSink`,
orchestrated by `Task`) has no automated end-to-end coverage. Unit tests
mock the edges; the live-cluster E2E client in `e2e/` is manual and depends
on shared TDMQ instances (console-created topics, foreign traffic — see
memory notes from 2026-08-15). A throwaway local broker makes the full
pipeline testable in CI-style runs.

## Scope

- **In:** MQTT-to-MQTT integration tests using the Testcontainers HiveMQ
  module, running under plain `mvn test` (user decision). Covers the real
  `Task` pipeline: MQTT subscribe/publish, manual ack, SQL transform,
  hop-count headers.
- **Out:** RocketMQ side (stays unit-tested; no official testcontainers
  module and containerizing the 5.x proxy is heavy), parallelism lanes,
  shared-subscription load-splitting, sink-retry behavior (needs fault
  injection; already covered by `TaskTest`).

## Dependencies (test scope, `pom.xml`)

```xml
<properties>
    <testcontainers.version>1.21.4</testcontainers.version>
</properties>
```

- `org.testcontainers:testcontainers:${testcontainers.version}`
- `org.testcontainers:junit-jupiter:${testcontainers.version}`
- `org.testcontainers:hivemq:${testcontainers.version}`

Verification clients use the existing compile-scope `hivemq-mqtt-client` —
no additional MQTT client dependency.

Verified 2026-08-16: `org.testcontainers:hivemq` exists up to 1.21.4 on
Maven Central (the module did not move to the 2.x `testcontainers-hivemq`
naming), and `hivemq/hivemq-ce:2026.5` is published on Docker Hub
(amd64+arm64). Local Docker 28.4.0 is available.

## Test Class

One class: `com.tencent.cloud.mqtt.MqttBridgeIntegrationTest` — named
`*Test` so surefire picks it up in default `mvn test` (per user decision
not to gate behind a profile).

**Container lifecycle:** `@Testcontainers` + `@Container static
HiveMQContainer` on `DockerImageName.parse("hivemq/hivemq-ce:2026.5")`. One
broker for the whole class; each test uses a unique topic prefix
(`it/<test-name>/...`) so tests stay independent without container
restarts.

**Per-test flow:**

1. Build a real `Task`: `MqttSource` (unique `it/<name>/in` filter),
   `SQLTransform` (test-specific SQL), `MqttSink` (`it/<name>/out`),
   against `hivemq.getHost()` / `hivemq.getMqttPort()`.
2. Launch the task on a virtual thread.
3. Publish input with one `Mqtt5BlockingClient`; subscribe to the output
   topic with another (via `publishes(MqttGlobalPublishFilter.ALL)`).
4. Assert with bounded timeouts (await up to ~10 s for positives, ~3 s
   observation window for negatives).
5. Interrupt the task thread and join it in a `finally`.

## Test Cases

1. **Happy path** — SQL with projection + `WHERE`; publish matching JSON;
   assert the transformed row arrives on the sink topic with
   `bridge-hop-count=1` user property, and key/other user properties
   preserved.
2. **Filtered out** — payload fails the `WHERE`; assert nothing arrives on
   the sink topic within the observation window (record is acked, not
   republished).
3. **Loop prevention** — publish with `bridge-hop-count` user property
   already above `max_hops`; assert dropped (nothing republished).
4. **Malformed JSON** — non-JSON payload; `SQLTransform` drops it; assert
   nothing arrives on the sink topic.

## Error Handling / Flakiness Controls

- All positive assertions use explicit timeouts (`receive(timeout)`);
  negative assertions use a short fixed window, not sleeps scattered
  through the test body.
- Task threads are always interrupted/joined in `finally` so a failing
  assertion doesn't leak a running task into the next test.
- Unique topic prefixes per test avoid cross-test message bleed; the
  container is ephemeral, so no cleanup of retained/session state is
  needed beyond that.
- Tests require Docker; environments without it will fail fast with
  Testcontainers' standard "could not find a valid Docker environment"
  error — accepted trade-off of running under default `mvn test`.
