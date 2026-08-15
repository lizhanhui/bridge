# RocketMQ Source & Sink — Design

Date: 2026-08-15
Status: Approved

## Goal

Implement the RocketMQ side of the bridge: a `RocketMQSource` consuming via
the RocketMQ 5.x `SimpleConsumer` (manual ack → crash-safe) and a
`RocketMQSink` publishing via the blocking 5.x `Producer` (fail-fast), wired
into `TaskManager` and the existing `AckableRecord` crash-safety contract.

## Dependency

`rocketmq-client-java` 5.2.1 (the 5.x gRPC proxy-protocol client with the
`apis` package) replaces the classic `rocketmq-client` 5.5.0, which has no
public manual-ack API (`DefaultMQPushConsumerImpl.ackAsync` is
package-private). The shade plugin's `ServicesResourceTransformer` already
covers the client's `ClientServiceProvider` ServiceLoader.

## Crash-safety contract

`SimpleConsumer.receive(1, invisibleDuration)` hands a message to the task
loop; the broker commits consumption only on `ack()`. A message that is never
acked (task crash, sink failure, shutdown) becomes visible again after the
invisible duration and is redelivered — at-least-once, same contract as the
MQTT side. `RocketMQAckableRecord` (core `AckableRecord` subtype) holds the
`SimpleConsumer` + `MessageView`; `doAck()` calls `consumer.ack(view)` and
logs on `ClientException` (failed ack ⇒ redelivery, safe).

Design choices:

- **One message per RPC, no local queue.** The invisible clock starts at
  receive; a batch sitting in a local queue could expire before its turn,
  causing premature redelivery and ack-after-expiry noise. Throughput scales
  via task lanes, which load-balance within the shared consumer group.
- **Constants:** `INVISIBLE_DURATION = 30s` (must cover one message's
  transform + publish), `AWAIT_DURATION = 3s` (long-poll window; also bounds
  shutdown latency, since `receive` may not respond to thread interrupt).
- **Failure model:** receive/publish `ClientException` → fail-fast (lane
  dies, unacked messages redeliver), consistent with `MqttSink`.

## Record mapping (`RocketMQRecordMapper`)

Plain RocketMQ mapping (decided against unpacking the rocketmq-mqtt broker's
`extData` envelope — the bridge's RocketMQ side is protocol-generic).

Source — `toRecord(MessageView)`:

| MessageView field | Record |
|---|---|
| body | value (UTF-8) |
| messageId | record **key** + `rmq.message.id` header |
| topic | `rmq.topic` header |
| tag | `rmq.tag` header (when present) |
| keys | `rmq.keys` header (comma-joined, when present) |
| bornTimestamp | record timestamp + `rmq.born.timestamp` header |
| deliveryTimestamp | `rmq.delivery.timestamp` header (when present) |
| deliveryAttempt | `rmq.delivery.attempt` header |
| bornHost | `rmq.born.host` header |
| every property | header with the same name |

Sink — `toMessage(record, defaultTopic)`: `rmq.topic` header overrides the
configured topic; `rmq.tag` / `rmq.keys` set tag/keys; reserved `rmq.*`
headers are consumed as settings and `$__`-prefixed headers are dropped
(broker-assigned MQTT metadata); every other header becomes a message
property. The record key is symmetric with the MQTT side: the broker's
`$__messageId` user property is the RocketMQ UNIQ_KEY (verified against
/data/repo/rocketmq-mqtt: `mqtt-ds/.../PublishProcessor.java:133`,
`mqtt-cs/.../Session.java:937`).

Properties colliding with reserved header names are skipped (logged) on the
source side.

## Config schema

RocketMQ source: `consumer_group` (string, required), `topics` (array of
strings, required — all tags, `FilterExpression.SUB_ALL`). RocketMQ sink:
`topic` (string, required). Connector `access_point` is the proxy
`host:port` (plaintext); `username`/`password` map to a
`StaticSessionCredentialsProvider` when set.

## Components

New package `com.tencent.cloud.mqtt.rocketmq`:

- `RocketMQClients` — shared client construction from `Connector`.
- `RocketMQRecordMapper` — both mapping directions (above).
- `RocketMQAckableRecord` — ack context.
- `RocketMQSource` — `poll()` loops `receive(1, INVISIBLE_DURATION)` until a
  message arrives or the source is closed.
- `RocketMQSink` — `publish()` sends blocking; `ClientException` propagates
  as RuntimeException (fail-fast).

`TaskManager` wires both branches; lanes of a task share the consumer group,
so `parallelism` load-balances natively (no shared-subscription caveat like
MQTT).

## Testing

`RocketMQRecordMapperTest` — offline (stub `MessageView`; real
`MessageBuilder` is network-free): field mapping, key/timestamp, property
round-trip, reserved-collision skip, `rmq.topic` override, reserved + `$__*`
exclusion on produce. No broker integration tests, consistent with the MQTT
side.
