# Authenticated Kafka End-to-End Integration Plan

> **For CodeBuddy:** Use test-driven-development and independent review checkpoints before completion.

**Goal:** Verify Kafka sinks then sources with Kafka authentication and retain repeatable, feature-focused broker-backed tests.

**Architecture:** The user approved isolated authenticated Kafka containers because the running standalone Kafka has only a PLAINTEXT client listener. Leave standalone Kafka, existing Mosquitto services, production code, and runtime conf/tasks.json unchanged. A small shared test-only KafkaContainer subclass generates ephemeral SASL/PLAIN credentials and fresh native client-property maps. Existing Kafka suites use these settings for bridge connectors and all admin/producer/consumer observers. KafkaBridgeIntegrationTest uses Mosquitto by default, with explicit host/port test properties to select an existing external MQTT broker without starting/stopping it.

**Tech stack:** Existing Kafka clients4.4.0, Apache Kafka3.7.0 Testcontainers fixture, cached pinned eclipse-mosquitto image, JUnit5 and Maven; no dependency additions.

## Task1: Require Kafka authentication in existing suites

Files: create src/test/java/com/tencent/cloud/mqtt/kafka/AuthenticatedKafkaContainer.java; modify KafkaSourceIntegrationTest.java and KafkaBridgeIntegrationTest.java.

1. Write real bad-password tests for KafkaSink publish, KafkaSource consumption and AdminClient readiness (constructors are lazy, so exercise real broker RPCs). Demonstrate RED on the current unauthenticated fixture before implementation; a generic timeout is not acceptable proof of rejected credentials.
2. Configure the default external listener with SASL_PLAINTEXT/PLAIN, preserving internal broker/controller listeners. Do not add withListener because it introduces plaintext listeners. Generate credentials in memory, expose only fresh property maps; no existing credentials/config files or raw JAAS logging.
3. Add native bootstrap/security.protocol/sasl.mechanism/sasl.jaas.config to EVERY existing client/connector path in both suites, then retain all existing success tests. Verify authenticated Admin readiness and explicit SaslAuthenticationException rejection without sensitive output.
4. Run KafkaSourceIntegrationTest and reviewed MQTT sink/source cases, self-review and independently review fixture/security boundaries.

## Task2: Mosquitto and feature gaps

Files: modify KafkaBridgeIntegrationTest.java and KafkaSourceIntegrationTest.java only.

1. Replace only the Kafka bridge suite's HiveMQ fixture with pinned Mosquitto configured in-memory for anonymous MQTT5. Optional bridge.it.mqtt.host and bridge.it.mqtt.port together select an existing broker; default CI creates/destroys only its own container. Source/sink topics are unique and not retained except where an explicit retained-setting assertion requires cleanup. Existing MQTT-only HiveMQ suite remains unchanged.
2. Add persistent broker assertions for malformed JSON skip and safe commits, multi-row SQL outputs, equality/malformed/existing hop counts, routing and source metadata, optional MQTT response/correlation/expiry/retain/duplicate properties within existing mapper contracts. Independently assert Kafka source group committed offsets on MQTT destination flows. Do not claim arbitrary binary/null Kafka headers round-trip through MQTT UTF-8 properties.
3. Add a real two-member/two-partition Kafka source rebalance test with one blocked active record, actual assignment checks, no early offset advance or stale-lease commit, then completion/replay. Preserve existing stalled-membership and restart tests. Add transactional read_committed visibility if practical as a bounded real-broker feature check. No broker restart or shared-service fault injection.
4. All tests use deadlines/latches, inspect all returned records, capture thread failures and close every owned client/thread even on assertion failure. Test only expected SASL authentication behavior, not TLS/ACL/SCRAM.

## Task3: Live verification and final review

1. The lead runs MQTT→Kafka sink first, then Kafka→MQTT source tests using the existing local Mosquitto test endpoint and the authenticated Kafka fixture. Independent Kafka clients use the same required SASL settings. Record exact assertions/results, not a process-liveness success.
2. Run portable authenticated suites with no external properties, then fresh mvn package; inspect reports and jar. Validate docs, diff, and unchanged runtime config hash.
3. Update README testing instructions and feature coverage matrix, explicitly separating tested authentication from TLS/ACL and unit-only or untested features. No commit/push is authorized.

Runtime configuration fingerprint to preserve: a56902bae8d77b01724c125fb30677aff5639f1956f7967fef71529dbd0cfd79.
