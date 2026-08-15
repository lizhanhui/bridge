# Production Readiness Issues

Review target: commit `2fad34186e04071a462816881bd24a01ba0a67f9`

Current rollout verdict: **No-go for production**

## Critical

### 1. Plaintext broker credentials are tracked

Status: **partially fixed**. The checked-in configs now contain template placeholders (`<mqtt-username>`, `<mqtt-password>`, etc.) instead of real-looking credentials and endpoints.

The previously committed credentials remain in git history and must still be treated as compromised.

Remaining remediation:

- Rotate the credentials that were previously committed.
- Purge exposed secrets from history where appropriate (e.g. history rewrite or repository rotation).
- Load real secrets from environment variables, mounted files, or a secret manager at runtime — the current config format has no interpolation.
- Add secret scanning to CI.

### 2. Message properties can override configured sink topics

Locations:

- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java:81-93`
- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java:107-113`
- `src/main/java/com/tencent/cloud/mqtt/rocketmq/RocketMQRecordMapper.java:70-76`
- `src/main/java/com/tencent/cloud/mqtt/rocketmq/RocketMQRecordMapper.java:93-99`

MQTT output trusts `mqtt.topic`, and RocketMQ output trusts `rmq.topic`. Source properties can therefore route messages using the bridge's producer credentials. Same-protocol forwarding also uses the source topic instead of the configured sink topic.

Required remediation:

- Make configured sink topics authoritative by default.
- If dynamic routing is required, make it an explicit option with an allowlist.
- Do not derive privileged routing fields from arbitrary source user properties.
- Add cross-protocol routing-injection tests.

## High

### 3. Processing failures permanently terminate a task lane

Status: **partially fixed**. Recoverable sink publication failures now retry indefinitely with exponential backoff from 1 second to 60 seconds, and sink validation failures are treated as poison records, skipped with a warning, and acknowledged. Source, transform, and acknowledgement failures still terminate a lane permanently.

Current locations:

- Sink retry and poison handling: `src/main/java/com/tencent/cloud/mqtt/Task.java:74-127`
- Lane error logging without restart: `src/main/java/com/tencent/cloud/mqtt/TaskManager.java:45-53`

Remaining risk: source, transform, and acknowledgement exceptions can escape the processing loop. `TaskManager` logs and swallows those failures, leaving the process alive with reduced or zero processing capacity.

Required remediation:

- Either terminate the process with a non-zero status so an external supervisor restarts it, or implement bounded lane restart with backoff.
- Expose lane health.
- Route poison records to a DLQ/quarantine instead of only logging and acknowledging them.
- Test manager behavior after a lane fails.

Known gap in the new classification: `RocketMQSink` wraps every `ClientException` — including client-side validation failures such as a bad topic from an `rmq.topic` header override — in a plain `RuntimeException` (`src/main/java/com/tencent/cloud/mqtt/rocketmq/RocketMQSink.java:35-38`). Such records are retried forever, head-of-line blocking the lane at a 60-second cadence. Broker-side rejections (e.g. not-authorized) on either sink have the same exposure. Operators should alert on repeated `sink publish failed; retrying` warnings. Consider unwrapping causes in `isPoisonSinkFailure` or adding a max-retry/DLQ policy.

### 4. MQTT queue saturation can exhaust inflight delivery

Location: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSource.java:56-69`

When the queue remains full for five seconds, the callback discards the only acknowledgement handle but leaves the connection open. Accumulated unacknowledged QoS 1/2 messages can consume all inflight slots and stall broker delivery indefinitely.

Required remediation:

- Do not discard an unacknowledged publish while retaining the connection.
- Apply receive flow control, wait safely for queue capacity, or fail/reconnect the lane.
- Add broker-backed queue-saturation and recovery tests.

### 5. RocketMQ invisibility is fixed and not renewed

Locations:

- `src/main/java/com/tencent/cloud/mqtt/rocketmq/RocketMQSource.java:35-36`
- `src/main/java/com/tencent/cloud/mqtt/rocketmq/RocketMQSource.java:55-58`

If transform and publication take longer than 30 seconds, the message becomes visible and can be processed concurrently by another lane. This causes duplicate and potentially amplified fan-out.

Required remediation:

- Make invisibility duration configurable.
- Renew invisibility while a message is being processed, or enforce a processing deadline safely below it.
- Add an integration test whose processing exceeds the initial duration.

### 6. At-least-once delivery does not apply to MQTT QoS 0

Locations:

- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSource.java:53-56`
- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttAckableRecord.java:18-20`
- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java:111-114`

A QoS 1 subscription does not upgrade a QoS 0 publication. There is no acknowledgement or broker redelivery after a crash, and the incoming QoS 0 value is propagated to the sink.

Required remediation:

- Enforce or document QoS 1 or higher as an ingress requirement.
- Avoid forwarding at QoS 0 when the task promises at-least-once behavior.
- Narrow the documented guarantee if QoS 0 must remain supported.

## Medium

### 7. Startup failures leak previously created lane resources

Locations:

- `src/main/java/com/tencent/cloud/mqtt/TaskManager.java:103-120`
- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSource.java:45-52`
- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttSink.java:21-26`

If construction of a later lane fails, only that lane's partial resources are closed. Clients held by earlier constructed tasks never enter `Task.launch()` and are not closed. MQTT connection failures can similarly leak a newly created client.

Required remediation:

- Treat startup as a resource transaction and close all previously constructed lanes in reverse order after any failure.
- Ensure connector constructors close clients when connection fails.
- Add staged startup-failure tests.

### 8. MQTT client IDs collide across replicas

Location: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttClients.java:38-48`

Client IDs include connector and lane names but no deployment-instance identity. Two replicas of the same configuration use identical IDs and can repeatedly disconnect each other. Identifier sanitization can also collapse distinct names to the same value.

Required remediation:

- Include a stable, configured instance identity.
- Validate uniqueness after sanitization.
- Keep source IDs stable for persistent sessions while ensuring concurrent replicas differ.

### 9. TLS is inferred only from port 8883

Location: `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttClients.java:22-64`

TLS brokers on other ports are contacted using plaintext. Credentials configured for a non-8883 endpoint can also be sent without encryption.

Required remediation:

- Add explicit TLS/transport configuration independent of port.
- Reject plaintext credentials by default unless explicitly permitted.
- Support required trust material configuration.

### 10. Loop-prevention metadata is weakly validated

Locations:

- `src/main/java/com/tencent/cloud/mqtt/Task.java:58-65`
- `src/main/java/com/tencent/cloud/mqtt/Task.java:80-97`
- `src/main/java/com/tencent/cloud/mqtt/TaskManager.java:77-86`

Malformed counts reset to zero, negative counts are accepted, integer overflow is possible, and `hopCount > maxHops` permits a record whose current count equals the maximum to make another bridge pass. Negative `max_hops` is not rejected.

Required remediation:

- Require a bounded, non-negative hop count.
- Reject or quarantine malformed values rather than resetting them.
- Validate `max_hops`.
- Define and test whether the limit applies before or after the next bridge pass.

### 11. Broker-property filtering drops all `$__*` properties

Locations:

- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java:43-45`
- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java:140-149`
- `src/main/java/com/tencent/cloud/mqtt/rocketmq/RocketMQRecordMapper.java:39-40`
- `src/main/java/com/tencent/cloud/mqtt/rocketmq/RocketMQRecordMapper.java:110-116`

The authoritative broker source defines five broker-generated annotations, not the entire `$__` namespace:

- `$__messageId`
- `$__publisherClientId`
- `$__publisherClientHost`
- `$__publisherUsername`
- `$__messageTimestamp`

Dropping the entire prefix silently loses legitimate application properties. Duplicate `$__messageId` properties can also cause the last value to become the record key instead of the broker-generated value.

Required remediation:

- Filter the exact broker-defined names rather than the whole prefix.
- Define deterministic handling for duplicate `$__messageId` values.
- Verify behavior against `/data/repo/rocketmq-mqtt/mqtt-common/src/main/java/org/apache/rocketmq/mqtt/common/model/Constants.java:46-50`.

### 12. Configuration parsing accepts ambiguous or invalid values

Locations:

- `src/main/java/com/tencent/cloud/mqtt/TaskManager.java:74-95`
- `src/main/java/com/tencent/cloud/mqtt/TaskManager.java:157-189`

Duplicate connector IDs overwrite earlier entries. Numeric coercion can silently apply defaults or truncate values, and blank names, endpoints, topics, and filters are accepted.

Required remediation:

- Add strict schema validation before constructing clients.
- Reject duplicate IDs, wrong JSON types, blanks, unknown fields, invalid ranges, and invalid topic/filter syntax.

### 13. Malformed payloads are acknowledged and logged in full

Locations:

- `src/main/java/com/tencent/cloud/mqtt/SQLTransform.java:53-60`
- `src/main/java/com/tencent/cloud/mqtt/Task.java:66-68`

Malformed JSON produces an empty transform result and is then acknowledged, causing irreversible loss. The complete payload is logged, which can expose sensitive data or enable log-volume abuse.

Required remediation:

- Add configurable rejection handling, preferably DLQ or quarantine.
- Log bounded/redacted identifiers or hashes rather than full payloads.

### 14. Shutdown does not wait for task cleanup

Location: `src/main/java/com/tencent/cloud/mqtt/TaskManager.java:59-66`

The shutdown hook interrupts task threads and immediately returns. JVM shutdown can proceed before source and sink cleanup completes.

Required remediation:

- Interrupt all lanes and wait for them with a bounded deadline.
- Force-close remaining resources after the deadline.
- Test the real shutdown path.

### 15. Transform output is materialized without limits

Location: `src/main/java/com/tencent/cloud/mqtt/SQLTransform.java:69-75`

A large payload and fan-out query can materialize an unbounded list before publishing, exhausting heap and terminating the JVM.

Required remediation:

- Add payload-size, result-row, and total-output-byte limits.
- Stream output rows where practical.
- Test maximum-size and fan-out behavior.

## Low

### 16. MQTT message expiry is restarted after bridge processing

Locations:

- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java:76-79`
- `src/main/java/com/tencent/cloud/mqtt/mqtt/MqttRecordMapper.java:129-137`

The received remaining expiry interval is copied unchanged to the outgoing publish. Time spent processing is not deducted, so the message can outlive the publisher's intended expiry.

Required remediation:

- Track an absolute expiry deadline internally.
- Recalculate remaining lifetime immediately before publication.
- Drop messages whose deadline has expired.

## Build and test gaps

Verified on the reviewed commit:

- `mvn test`: 42 tests passed, 0 failures.
- `mvn clean package -DskipTests`: build succeeded.
- The shaded build reported overlapping classes/resources, including LZ4 and Zstd copies involving `rocketmq-client-java`.

Before rollout, add broker-backed tests covering:

- MQTT QoS 0 and QoS 1 crash behavior.
- MQTT queue saturation, reconnect, and persistent-session redelivery.
- RocketMQ invisibility expiry and acknowledgement failure.
- Multi-lane and multi-replica operation.
- Sink-topic injection attempts.
- Poison-message and lane-failure supervision.
- SIGTERM shutdown.
- Startup rollback after partial connector construction.
- Running both connector types from the final shaded JAR.
