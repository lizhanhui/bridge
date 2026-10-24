# Kafka Sink Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Allow MQTT and RocketMQ tasks to publish transformed records to a Kafka sink with native connector configuration and safe optional defaults.

**Architecture:** Add native producer properties to Connector and parse them only for Kafka. Each KafkaSink owns a producer, maps records without metadata loss, and waits for broker confirmation before Task acknowledges its source. Reuse existing retry/poison behavior without changing Task semantics.

**Tech Stack:** Java 25, Maven, Kafka clients 4.4.0, Kafka Streams Record types 4.4.0 (version alignment approved at commit preparation), JUnit 5, MockProducer, Testcontainers 2.0.6-rocketmq-SNAPSHOT.

---

### Task 1: Kafka production implementation and unit coverage

**Files:**
- Modify: `src/main/java/com/tencent/cloud/mqtt/model/Connector.java`, `ConnectorType.java`, `src/main/java/com/tencent/cloud/mqtt/TaskManager.java`, existing `src/main/java/com/tencent/cloud/mqtt/kafka/KafkaSink.java`.
- Create: `src/main/java/com/tencent/cloud/mqtt/kafka/KafkaClients.java`, `KafkaRecordMapper.java`.
- Tests: `src/test/java/com/tencent/cloud/mqtt/TaskManagerTest.java`, new `src/test/java/com/tencent/cloud/mqtt/kafka/KafkaClientsTest.java`, `KafkaRecordMapperTest.java`, `KafkaSinkTest.java`.

1. Write failing configuration, mapper, and sink tests before implementing each behavior. Run `mvn test -Dtest=TaskManagerTest,KafkaClientsTest,KafkaRecordMapperTest,KafkaSinkTest`, confirming missing-feature failures. Use an injected Producer for unit tests; keep test-only state out of production.
2. Add `Kafka` enum and `Map<String,Object>` native properties to Connector. Parser must accept a Kafka connector without access_point, require a properties object with native bootstrap.servers, retain native values (including list-valued settings), and preserve existing schemas. Reject Kafka source configuration explicitly.
3. Build producer settings by copying user properties, rejecting serializer overrides/transactional.id/acks=0, then applying absent defaults:

```java
properties.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
properties.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
properties.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, clientId);
properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
```

Use Kafka validation for native option types/conflicts; do not log raw properties or credentials. Test defaults/overrides, scalar/list types, restrictions, and meaningful errors.
4. Map key/value/timestamp and copied Headers with partition null. Last `dst.kafka.topic` wins, null routing value falls back to default, all routing headers are removed from the outgoing copy, every other header is retained. Tests cover binary/null/duplicate headers, source annotations and input immutability.
5. Implement KafkaSink public `(Connector, String topic, String clientId)` constructor plus package-private injected-producer seam, blocking completion and error unwrapping:

```java
producer.send(KafkaRecordMapper.toProducerRecord(record, topic)).get();
```

Restore interruption before propagating it. Classify only record-specific permanent errors as IllegalArgumentException; preserve transient/security failures. Bound close with `Duration.ofSeconds(10)`. Test synchronous/asynchronous errors, interruption while waiting, close, and real Task ack ordering using MockProducer.
6. Wire KafkaSink in TaskManager. Add offline parser/source rejection tests and verify focused tests are green. Self-review, then obtain independent spec review and code-quality review before marking task complete.

### Task 2: Container integration coverage

**Files:** create `src/test/java/com/tencent/cloud/mqtt/KafkaBridgeIntegrationTest.java`; dependency setup in `pom.xml` is owned by coordinator.

1. Use matching Testcontainers `org.testcontainers.kafka.KafkaContainer` with the cached Apache Kafka image, plus HiveMQContainer and RocketMQContainer following existing fixtures. No shared clusters or running services are modified.
2. Write bounded MQTT→Kafka and RocketMQ→Kafka tests against real Task/SQLTransform/KafkaSink, creating source/output topics as required. Cover projection, source key/provenance/protocol metadata and hop count, plus dst.kafka.topic routing and non-propagation. Assign a KafkaConsumer to known topic partitions for deterministic verification. Capture lane failures and always close clients/interrupt and join task threads.
3. Run `mvn test -Dtest=KafkaBridgeIntegrationTest`. Investigate actual container/client failures without bypassing tests or changing existing production source behavior.
4. Obtain spec and code-quality reviews.

### Task 3: Documentation, dependencies, and final verification

**Files:** modify `pom.xml`, `README.md`, `conf/tasks.json.template`; keep runtime `conf/tasks.json` untouched.

1. Consolidate duplicate clients dependencies to the user's 4.4.0 declaration. Publish local Kafka Testcontainers module under the configured snapshot version, add test-only dependency, and check dependency tree.
2. Update README flow list, architecture, connector fields/examples/defaults/security, sink config, Kafka mapping/routing/poison behavior, delivery caveats and test prerequisites. Add Kafka connector and MQTT→Kafka/RocketMQ→Kafka tasks to the template; validate template JSON.
3. Run `mvn test`, then `mvn -DskipTests package`; confirm test summaries, runnable jar creation and no duplicate Kafka dependency declarations. Run `git diff --check` and verify runtime config hash is unchanged from the start.
4. Obtain final review, address concrete issues test-first, rerun verification, and report concise results. No commits or pushes are authorized.
