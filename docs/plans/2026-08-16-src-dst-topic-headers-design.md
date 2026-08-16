# src/dst Topic Header Split Design

Date: 2026-08-16
Status: Approved

## Problem

Both protocol mappers use one header for two conflicting roles: `mqtt.topic`
(and `rmq.topic`) is set on consume to record where a message came from, AND
honored on produce as an override of the sink's configured topic. Any
same-protocol pipeline (MQTT→MQTT, RocketMQ→RocketMQ) therefore loops its
output back onto the input topic. Commit `80a1268` patched this for MQTT by
force-overriding the topic in `MqttSink` after mapping; the RocketMQ side
remains latently broken, and the patch leaves the mapper's documented
"header overrides configured topic" contract dead in production.

## Decision

Split the two roles into explicit headers, symmetric across protocols
(user-confirmed 2026-08-16):

| Header | Set by | Consumed by | Propagates downstream? |
|---|---|---|---|
| `src.mqtt.topic` / `src.rmq.topic` | mapper `toRecord` (source topic) | nobody — informational | **Yes**, as ordinary user property / message property; each hop overwrites it with its own source topic |
| `dst.mqtt.topic` / `dst.rmq.topic` | user or upstream bridge (deliberate routing) | mapper `toPublish`/`toMessage` as topic override over the sink's configured topic | No — consumed at the sink |

The bare `mqtt.topic` / `rmq.topic` header names disappear entirely.

Scope decisions (user-confirmed):
- **Symmetric**: applied to both `MqttRecordMapper` and `RocketMQRecordMapper`.
- **`src.*` propagates**: provenance is visible to downstream consumers.
- **Topic only**: `mqtt.qos` / `mqtt.retained` keep their current
  override semantics; the QoS-downgrade question is deferred.

## Mapper Changes

The single `RESERVED_HEADERS` set currently serves two directions; it splits
by role:

- **Consume-shadow set** (user properties with these names are skipped on
  `toRecord` so they cannot shadow real protocol data): the existing
  reserved set with `mqtt.topic` replaced by `src.mqtt.topic`
  (likewise `rmq.topic` → `src.rmq.topic`).
- **Publish-consumed set** (headers used as publish settings / overrides,
  not propagated): the existing reserved set minus `src.*.topic` (so
  `src.*` propagates) plus `dst.*.topic` (so the override is consumed).

`toRecord` sets `src.*.topic` instead of `*.topic`. `toPublish`/`toMessage`
resolve the topic as `dst.*.topic` header, else the sink's configured
default. A user property/property literally named `dst.*.topic` arriving
from outside is NOT reserved on consume — it becomes the header and thereby
reroutes the message, which is exactly the intended override channel.

`MqttSink.publish` reverts the `80a1268` force-override
(`.extend().topic(topic)`): with the collision gone, the mapper's
`defaultTopic` path is correct again — configured topic wins unless a
`dst.mqtt.topic` header deliberately overrides it.

`docs/plans/2026-08-15-mqtt-record-headers.md` (header-mapping reference)
is updated to match.

## Cross-Protocol Propagation

`src.*` headers are non-reserved on the opposite protocol's publish path,
so provenance survives protocol crossings: MQTT→RMQ→MQTT yields an
`src.rmq.topic` user property on the final publish (the immediate hop's
source), etc. Hop-count loop prevention is unaffected.

## Testing

- **Mapper unit tests** (`MqttRecordMapperTest`, `RocketMQRecordMapperTest`):
  - consume sets `src.*.topic`; a user property named `src.*.topic` is
    skipped (shadow protection);
  - produce honors `dst.*.topic` over the default topic; `dst.*.topic` is
    consumed (not propagated as a property);
  - `src.*.topic` header IS propagated as a user property/message property
    on produce;
  - the old bare `*.topic` header no longer overrides anything (treated as
    an ordinary property).
- **Integration test** (`MqttBridgeIntegrationTest`):
  - happy path gains an assertion that the sink output carries
    `src.mqtt.topic == "it/happy/in"` (provenance round-trip);
  - new test: publishing with a `dst.mqtt.topic` user property reroutes the
    message to the named topic instead of the sink's configured topic.
- The existing `assertEquals("it/happy/out", received.getTopic())`
  assertion continues to guard against input-topic echo after the
  `MqttSink` force-override is reverted.
