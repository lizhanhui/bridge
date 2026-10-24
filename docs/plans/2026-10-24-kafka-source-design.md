# Kafka Source Design

Approved 2026-10-24 for implementation on the existing `feat/kafka-source` branch. Preserve the user's runtime `conf/tasks.json`; no commits or pushes are authorized by this implementation request.

## User decisions

- Prefer higher throughput using buffered fetching and batched offset commits, not one commit RPC per record.
- Require explicit native `auto.offset.reset`; do not assume earliest or latest.
- Skip null-valued Kafka tombstones with a warning and completed-offset accounting; keep SQLTransform unchanged.
- Default `isolation.level=read_committed`, with native override allowed.

## Configuration

Existing Kafka connectors work as sources and sinks. Kafka-native properties stay under connector.properties; bootstrap.servers remains required, and sources additionally require nonblank group.id and explicit auto.offset.reset. All source tasks sharing group.id compete for work; independent fan-out uses different groups. Source topics are a nonempty array of exact Kafka topic names, not regex/manual assignments.


```json
{"id":"kafka-in","type":"Kafka","properties":{"bootstrap.servers":"localhost:9092","group.id":"bridge-workers","auto.offset.reset":"earliest"}}
```

```json
{"connector_id":"kafka-in","topics":["events"],"commit_interval_ms":1000}
```

Consumer defaults: enable.auto.commit=false, source-<lane> client.id, StringDeserializer key/value (UTF-8 by default), isolation.level=read_committed. Native max.poll.records stays configurable, default500; other native settings retain Kafka defaults. Reject auto-commit=true, deserializer overrides, static group.instance.id, nonempty consumer interceptor.classes, and group.protocol other than classic initially. The classic-only boundary avoids Kafka4.4 async-consumer calls with hidden default.api.timeout waits; static IDs and mutating interceptors threaten ownership/offset assumptions. Preserve existing producer restrictions and unrelated role properties; source-only validation does not reject producer serializer/settings. A shared connector is valid for compatible common settings, though role-specific plugins/shared client.id can warrant separate connectors.

Source commit_interval_ms is a positive integer milliseconds, default1000, not native auto.commit.interval.ms (automatic commits are disabled). All MQTT/RocketMQ connector/schema behavior remains unchanged; no dependency additions are needed.

## Processing and bounded buffering

One dedicated consumer-owner virtual thread per lane owns construction, subscribe, poll, pause/resume, commit, and close; cross-thread wakeup is reserved for shutdown. Owner polls roughly every100ms even during sink retries. One monitor protects a deque containing at most one native poll batch, one active task record, per-partition ownership leases, and completed/committed offset watermarks. Pause every assigned partition while buffered or active work exists, and reapply pauses after assignments; resume fetching only after the batch and active work drain. Native fetch caches are separate: max.poll.records does not bound Kafka's underlying byte buffers; use native fetch settings to tune those.

Task processing stays sequential per lane, preserving source partition order; task.parallelism and Kafka partitions supply concurrency. Task ack marks the record completed in memory, clears only its own active token, and returns without waiting for an offset RPC. Buffered ack-to-next-poll does not wait for owner polling. Owner periodically snapshots completed>committed next offsets and commits that explicit map outside the monitor, never fetched positions or an entire unprocessed batch. Advance committed watermarks only to the successful snapshot and only for the same lease. Ack during a commit leaves newer completion dirty. Include leader epoch where appropriate.

Tombstones use the same local completion accounting in Source.poll and never enter SQLTransform. Filtered, hop-dropped, and poison-skipped records likewise count as completed through existing Task semantics.

## Rebalances, failures, and shutdown

Use partition-local ownership-instance leases; preserve continuously retained cooperative partitions. Revoke atomically snapshots completed revoked-prefixes, invalidates affected leases and removes their queued records, retaining an old active token until its actual ack. Best-effort bounded revoke commit occurs outside the monitor; lost does not commit. Old delivered work may finish, but stale ack only warns and releases its token: it never advances a new assignment's offsets. New assignments pause while old buffered/active work remains. Normal rebalance does not interrupt a task; external sink effects may duplicate.

Periodic timeout/retriable/RebalanceInProgress/CommitFailed errors retain dirty state and return to poll; each next attempt re-snapshots current leases rather than replaying stale maps. Auth/authorization/fencing and unexpected consumer failures stop the source, publish the original error, wake waiters, and interrupt the captured Task thread so infinite sink retries cannot hide source failure. Initialization uses a success/failure handshake and closes partial clients.

Use short explicit commit budgets (up to1s, adapted below max.poll.interval.ms) and short polls; no synchronous retry loop starving membership. Close stops new delivery, wakes owner, and best-effort flushes only valid completed prefixes before owner-only bounded consumer close; unprocessed active/queued offsets are never committed. Join with one deadline and preserve task interruption. Close callbacks must not fetch/resume/dispatch new records.

## Mapping and delivery semantics

Inbound mapping preserves key (including null), non-null JSON string value, nonnegative timestamp, ordered duplicate/binary/null headers, hop count, dst.* routing hints, cross-protocol metadata and $__* annotations. Negative/no timestamp falls back to receive time because bridge Record rejects negative timestamps. Regenerate authoritative src.kafka.topic, kafka.partition, kafka.offset headers, dropping only conflicting copies of these generated metadata names; partition/offset are informational, not destination routing.

Batched ack differs from durable Kafka offset commit. Processing is at-least-once: a crash, rebalance or failed final flush may replay completed messages since the last successful commit, plus unfinished work. Sink idempotence does not eliminate bridge replay duplicates. A stalled sink blocks a lane and its newly assigned partitions even while owner polling keeps group membership; scale lanes/partitions rather than hiding this limitation.

## Verification

Test native source config/defaults/explicit reset and shared-role independence; metadata cloning/collision/fallback; multirecord/multipartition prefix commits and compaction gaps; fast ack while commit is blocked; completion during snapshot; revoke/lost/reassign-same-partition and cooperative retained work; paused new assignments; transient commit recovery with fresh snapshots; tombstone order; fatal owner during sink retry; interrupted/idempotent close with no unprocessed commits; startup rollback. Real isolated broker tests cover Kafka→MQTT/RocketMQ/Kafka, transformation/filter/routing/headers, completed offset/restart replay and continued group polling during blocked processing. Full Maven tests/package and independent spec/quality reviews gate completion. Runtime config fingerprint: a56902bae8d77b01724c125fb30677aff5639f1956f7967fef71529dbd0cfd79.
