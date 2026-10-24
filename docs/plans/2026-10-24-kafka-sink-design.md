# Kafka Sink Design

Approved 2026-10-24. Implement on the existing feature branch, preserving user edits; no commits or pushes.

## Scope and configuration

Add sink-only connector type `Kafka` for MQTT/RocketMQ sources. Keep native Kafka settings under connector `properties`; `bootstrap.servers` is required and Kafka does not require `access_point`. Task sink still requires `connector_id` and `topic`. Existing MQTT/RocketMQ schemas and runtime `conf/tasks.json` remain unchanged.

```json
{"id":"kafka-example","type":"Kafka","properties":{"bootstrap.servers":"kafka:9092","compression.type":"lz4"}}
```

Use native values and property names for security/SASL/TLS and tuning; do not translate top-level username/password. Reject malformed property objects and missing/empty bootstrap settings at startup. String serializers are bridge-owned. Default `acks=all`, `enable.idempotence=true`, and `client.id=sink-<lane>`; preserve Kafka defaults for other settings (delivery timeout 120s, metadata/buffer wait 60s). Native overrides are allowed except serializers, `acks=0`, and unsupported `transactional.id`. Kafka used as a source is rejected clearly.

## Producer and delivery

Each lane owns one producer. Publication blocks on send completion before source acknowledgement, with native delivery bounds. Preserve interruption so unfinished publication is not acknowledged. Close the producer with a 10-second bound. Delivery remains at-least-once: idempotence protects producer internal retries, not bridge resends or source replay. RocketMQ's existing fixed 30-second invisibility is unchanged.

## Mapping

Copy key, value, timestamp, and native headers to ProducerRecord; leave partition unset. Preserve header order, duplicates, binary/null values, source provenance, hop count, protocol metadata, and `$__*` annotations. `dst.kafka.topic` overrides the configured topic and is omitted only from the outgoing header copy; never mutate input headers. This follows existing trusted-upstream routing semantics, with Kafka ACLs as the boundary. Source mapping semantics and MQTT wire filtering do not change; current broker source was checked.

## Failures

Convert record-specific permanent errors (invalid topic/timestamp, record too large, serialization failure) into IllegalArgumentException for existing Task poison-skip behavior. Connectivity, timeout, authentication, and authorization failures remain retryable under the existing Task loop without acknowledging the source. Propagate synchronous and asynchronous producer errors consistently. Transactions and Kafka sources are out of scope.

## Dependencies, docs, and tests

Preserve the user's kafka-clients 4.4.0 addition; remove the earlier duplicate declaration. At commit preparation, the user approved aligning kafka-streams to 4.4.0 for Record types. Publish the matching local Testcontainers Kafka snapshot module and add a test-scoped dependency. Update README and the configuration template with both new flows, defaults, authentication, mapping, restrictions, and delivery caveats.

Use broker-free tests for configuration/defaults, mapping, producer completion/error/interrupt/close behavior, and publication-before-ack. Add isolated Kafka/HiveMQ/RocketMQ container integration coverage for transformed messages from both sources, provenance and routing. Run all tests and package verification, then review changes. Baseline after the user freed port 8080: 62 tests passed.
