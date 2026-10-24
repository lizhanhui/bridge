# Kafka Source Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add high-throughput Kafka sources feeding MQTT, RocketMQ and Kafka sinks with explicit reset policy, buffered processing and safe batched completed-offset commits.

**Architecture:** A consumer-owner thread fetches one bounded poll batch and keeps polling paused assignments while one Task record is processed. Acknowledgement records completion in memory; periodic explicit prefix commits are fenced by per-partition ownership leases. Existing Task and SQLTransform behavior stays unchanged.

**Tech Stack:** Java25, Kafka clients/Streams4.4.0, Maven, JUnit5, MockConsumer and existing Testcontainers Kafka/HiveMQ/RocketMQ snapshot modules.

---

### Task1: Native consumer config and mapping

**Modify:** src/main/java/com/tencent/cloud/mqtt/kafka/KafkaClients.java; KafkaRecordMapper.java; src/test/java/com/tencent/cloud/mqtt/kafka/KafkaClientsTest.java; KafkaRecordMapperTest.java.

1. Add failing tests for consumerConfig/createConsumer signatures, explicit group.id/reset, auto-commit/deserializer safeguards, classic/static/interceptor boundaries, native scalar/list security, read_committed/clientId defaults and producer/consumer independence. Add inbound mapping tests for key/timestamp/authoritative src.kafka.topic,kafka.partition,kafka.offset, binary/duplicate/null headers and no aliasing.
2. Run mvn test -Dtest=KafkaClientsTest,KafkaRecordMapperTest; scaffold signatures only if needed, obtain compiling missing-behavior RED before implementation.
3. Implement consumerConfig copying native properties and source-only validation; preserve producer behavior. Add createConsumer factory and toRecord(ConsumerRecord<String,String>) mapping without modifying outbound behavior. Any shared copy helper is justified only for existing producer/newconsumer repeated validation, not broad refactor.
4. Re-run focused tests GREEN and existing KafkaSinkTest; obtain spec then code-quality review before source implementation uses stable APIs.

### Task2: Buffered KafkaSource and task wiring

**Create:** src/main/java/com/tencent/cloud/mqtt/kafka/KafkaSource.java; KafkaAckableRecord.java; src/test/java/com/tencent/cloud/mqtt/kafka/KafkaSourceTest.java.
**Modify:** src/main/java/com/tencent/cloud/mqtt/TaskManager.java; src/test/java/com/tencent/cloud/mqtt/TaskManagerTest.java.

1. Write failing tests for public KafkaSource(Connector,List<String>,String clientId,long commitIntervalMillis) and a minimal injected Consumer seam. Cover one fetched batch/singleactive fast ack; explicit per-partition processed prefixes and compaction gaps; unprocessed exclusion; concurrent snapshot/ack; tombstones; lease-safe revoked/lost/reassigned/cooperative retained partitions; pause onnewassignment; short periodic commit recovery; ownerfatalinterrupt; initialized cleanup; interrupted/idempotent boundedclose. Capture worker Throwable and deterministic latches, not timing-only assertions.
2. Run mvn test -Dtest=KafkaSourceTest,TaskManagerTest, scaffolding only minimal missing API before behavioral RED.
3. Implement monitor/deque/active/perpartitionleases and consumerowner loop from approved design. Keep native consumer calls outside monitor and on owner except wakeup. Ack performs no RPC. Periodic commits snapshot only completion; transient failures return to poll with dirty state, fatal ones publish original failure+interrupttask. Revoke invalidates before best-effort safeflush; lost neverflush; stale activeacknever updates newlease. No Task/SQLTransform refactor.
4. Wire source.topics exactarray validation and optional positive integral commit_interval_ms default1000; group.id/reset validation lives in consumerConfig, not connector parsing. Replace obsolete Kafka-source rejection test with pure sourcefield validation tests (package-private helper as needed), retain all old MQTT/RMQ cases.
5. Run focused source/config/mapper/sink/Task tests GREEN, selfreview, spec and quality review; correct concrete findings test-first.

### Task3: Broker-backed Kafka source coverage

**Modify:** src/test/java/com/tencent/cloud/mqtt/KafkaBridgeIntegrationTest.java for Kafka→MQTT/RMQ flows or create one targeted KafkaSourceIntegrationTest.java for Kafka-only offset/restart/polling cases.

1. Use realKafka/Task/SQLTransform and existing MQTT/RMQ isolatedcontainers; source groups unique, explicitearliest,manualcommit. Createallrequired topics before eagerRMQ clients.
2. Test projections/filtering/metadata/hop/routing for Kafka→MQTT and Kafka→RMQ; verify binary headers onlywheretargetsupports, don'tdemandMQTT/RMQ binary-nullpreservation beyondtheircurrentcontracts. TestKafka→Kafka richheaders/nativeconfigsharedconnector.
3. Test real committedoffset advancement onlyafterpublication, tombstonefollowedbyvalidJSON, orderlyshutdown/restart, and interruptedblockedpublication replayfromsamegroup. Testshortmax.poll.interval withblockedprocessingbutcontinuedownerpolling. No fixed sleeps as sole correctness proof; usebrokercommittedoffset queries andlatches/deadlines.
4. Run dedicated suites after core releasesMaven; investigatefailures, no sharedcluster/no relatedprocesschanges. Obtain reviews.

### Task4: Docs/template and final verification

**Modify:** README.md; CODEBUDDY.md; conf/tasks.json.template. Keep conf/tasks.json untouched; no pom dependencychange needed.

1. Documentnineprotocolflows/nativegroup/reset/sourcefields/defaults andinitialsafeguards, compatibleconnectorreuse/fanout,batchedackreplaywindow/blockedlane,sourceheaders/tombstones/negative timestampfallback andtestprerequisites. Updategeneral ackcontract docs onlywhereKafka batching makes olddurableclaims inaccurate; don'trefactorlogic.
2. Validate READMEJSONblocks/templateJSON and connectorrefs. Run gitdiff --check andverifyruntimehash.
3. After allstablechanges, independently run mvn package, confirmfulltestcounts/noerrors andshadedjarcontainsKafkaSource,verifybranch feat/kafka-source. Reviewfinalspecandquality. No commit/pushiscurrentlyauthorized.
