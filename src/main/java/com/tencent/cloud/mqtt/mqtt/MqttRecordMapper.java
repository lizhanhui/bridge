package com.tencent.cloud.mqtt.mqtt;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserPropertiesBuilder;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder;

/**
 * Maps between MQTT 5 publishes and bridge records. Protocol info travels in
 * {@code mqtt.*} headers; user properties round-trip as headers with the same
 * name. The record key is the {@value #USER_PROPERTY_MESSAGE_ID} user property.
 * The source topic travels in {@value #H_SRC_TOPIC}: informational, set on
 * consume, and propagated downstream as a user property. The sink topic can be
 * deliberately overridden via {@value #H_DST_TOPIC}: it is consumed on publish
 * and never propagated. On publish, {@value #BROKER_PROPERTY_PREFIX}-prefixed
 * headers are dropped — they are broker-assigned and the broker sets fresh ones
 * on delivery.
 */
public final class MqttRecordMapper {
    private static final Logger log = LoggerFactory.getLogger(MqttRecordMapper.class);

    /** Informational source-topic header: set on consume, propagates downstream as a user property. */
    public static final String H_SRC_TOPIC = "src.mqtt.topic";
    /** Deliberate sink-topic routing override: consumed on publish, not propagated. */
    public static final String H_DST_TOPIC = "dst.mqtt.topic";
    public static final String H_QOS = "mqtt.qos";
    public static final String H_RETAINED = "mqtt.retained";
    public static final String H_DUPLICATE = "mqtt.duplicate";
    /** Defined for completeness; never populated (packet IDs are not exposed on subscribe). */
    public static final String H_PACKET_ID = "mqtt.message.packet.id";
    public static final String H_CONTENT_TYPE = "mqtt.content.type";
    public static final String H_CORRELATION_DATA = "mqtt.correlation.data";
    public static final String H_RESPONSE_TOPIC = "mqtt.response.topic";
    public static final String H_MESSAGE_EXPIRY_INTERVAL = "mqtt.message.expiry.interval";

    public static final String USER_PROPERTY_MESSAGE_ID = "$__messageId";
    public static final String BROKER_PROPERTY_PREFIX = "$__";

    private static final Set<String> RESERVED_HEADERS = Set.of(
        H_SRC_TOPIC, H_QOS, H_RETAINED, H_DUPLICATE, H_PACKET_ID,
        H_CONTENT_TYPE, H_CORRELATION_DATA, H_RESPONSE_TOPIC, H_MESSAGE_EXPIRY_INTERVAL);

    /**
     * Headers consumed as publish settings and excluded from user properties:
     * the consume-reserved set minus {@code src.mqtt.topic} (which propagates)
     * plus {@code dst.mqtt.topic} (which is consumed for routing).
     */
    private static final Set<String> PUBLISH_CONSUMED_HEADERS = Set.of(
        H_QOS, H_RETAINED, H_DUPLICATE, H_PACKET_ID,
        H_CONTENT_TYPE, H_CORRELATION_DATA, H_RESPONSE_TOPIC, H_MESSAGE_EXPIRY_INTERVAL,
        H_DST_TOPIC);

    private MqttRecordMapper() {
    }

    /**
     * Maps an incoming publish to a record. The key is the value of the
     * {@value #USER_PROPERTY_MESSAGE_ID} user property, or {@code null} when absent.
     * User properties whose names collide with reserved {@code mqtt.*} headers are
     * skipped (with a warning) so they cannot shadow the real protocol headers.
     * Timestamp is receive time.
     */
    public static Record<String, String> toRecord(Mqtt5Publish publish) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(H_SRC_TOPIC, utf8(publish.getTopic().toString()));
        headers.add(H_QOS, utf8(String.valueOf(publish.getQos().getCode())));
        headers.add(H_RETAINED, utf8(Boolean.toString(publish.isRetain())));
        // hivemq 1.3.17 does not expose the dup flag on delivered publishes
        // (it exists only on the internal MqttStatefulPublish), so always false.
        headers.add(H_DUPLICATE, utf8("false"));
        publish.getContentType().ifPresent(t -> headers.add(H_CONTENT_TYPE, utf8(t.toString())));
        publish.getCorrelationData().ifPresent(buf -> {
            ByteBuffer copy = buf.duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            headers.add(H_CORRELATION_DATA, bytes);
        });
        publish.getResponseTopic().ifPresent(t -> headers.add(H_RESPONSE_TOPIC, utf8(t.toString())));
        OptionalLong expiry = publish.getMessageExpiryInterval();
        if (expiry.isPresent()) {
            headers.add(H_MESSAGE_EXPIRY_INTERVAL, utf8(Long.toString(expiry.getAsLong())));
        }

        String key = null;
        for (Mqtt5UserProperty property : publish.getUserProperties().asList()) {
            String name = property.getName().toString();
            if (RESERVED_HEADERS.contains(name)) {
                log.warn("Skipping user property '{}': reserved record header name", name);
                continue;
            }
            String value = property.getValue().toString();
            headers.add(name, utf8(value));
            if (USER_PROPERTY_MESSAGE_ID.equals(name)) {
                key = value;
            }
        }

        String value = new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8);
        return new Record<>(key, value, System.currentTimeMillis(), headers);
    }

    /**
     * Maps a record to an outgoing publish. Present headers override the configured
     * defaults ({@code dst.mqtt.topic} over {@code defaultTopic}, {@code mqtt.qos} over
     * {@code defaultQos}, etc.); malformed or out-of-range header values log a warning
     * and fall back to the default (or are skipped). The {@code dst.mqtt.topic} and
     * {@code mqtt.*} setting headers are consumed as publish settings and
     * {@value #BROKER_PROPERTY_PREFIX}-prefixed headers are dropped; every other header
     * — including {@code src.mqtt.topic} — becomes a user property.
     */
    public static Mqtt5Publish toPublish(Record<String, String> record, String defaultTopic,
            MqttQos defaultQos) {
        Headers headers = record.headers();

        Mqtt5PublishBuilder.Complete builder = Mqtt5Publish.builder()
            .topic(stringHeader(headers, H_DST_TOPIC, defaultTopic))
            .qos(qosHeader(headers, defaultQos))
            .retain(retainedHeader(headers))
            .payload(record.value().getBytes(StandardCharsets.UTF_8));

        String contentType = stringHeader(headers, H_CONTENT_TYPE, null);
        if (contentType != null) {
            builder.contentType(contentType);
        }
        Header correlationData = header(headers, H_CORRELATION_DATA);
        if (correlationData != null && correlationData.value() != null) {
            builder.correlationData(ByteBuffer.wrap(correlationData.value()));
        }
        String responseTopic = stringHeader(headers, H_RESPONSE_TOPIC, null);
        if (responseTopic != null) {
            builder.responseTopic(responseTopic);
        }
        String expiry = stringHeader(headers, H_MESSAGE_EXPIRY_INTERVAL, null);
        if (expiry != null) {
            try {
                // IllegalArgumentException covers both non-numeric input
                // (NumberFormatException) and values outside 0..4294967295
                builder.messageExpiryInterval(Long.parseLong(expiry));
            } catch (IllegalArgumentException e) {
                log.warn("Malformed {} header '{}', skipping", H_MESSAGE_EXPIRY_INTERVAL, expiry);
            }
        }

        if (headers != null) {
            Mqtt5UserPropertiesBuilder propsBuilder = Mqtt5UserProperties.builder();
            boolean hasProps = false;
            for (Header header : headers) {
                String name = header.key();
                if (!PUBLISH_CONSUMED_HEADERS.contains(name) && !name.startsWith(BROKER_PROPERTY_PREFIX)
                        && header.value() != null) {
                    propsBuilder.add(name, new String(header.value(), StandardCharsets.UTF_8));
                    hasProps = true;
                }
            }
            if (hasProps) {
                builder.userProperties(propsBuilder.build());
            }
        }

        return builder.build();
    }

    private static boolean retainedHeader(Headers headers) {
        String value = stringHeader(headers, H_RETAINED, null);
        if (value == null) {
            return false;
        }
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            log.warn("Malformed {} header '{}', treating as false", H_RETAINED, value);
        }
        return "true".equalsIgnoreCase(value);
    }

    private static MqttQos qosHeader(Headers headers, MqttQos defaultQos) {
        String value = stringHeader(headers, H_QOS, null);
        if (value == null) {
            return defaultQos;
        }
        try {
            MqttQos qos = MqttQos.fromCode(Integer.parseInt(value));
            return qos != null ? qos : warnAndDefault(value, defaultQos);
        } catch (NumberFormatException e) {
            return warnAndDefault(value, defaultQos);
        }
    }

    private static MqttQos warnAndDefault(String value, MqttQos defaultQos) {
        log.warn("Malformed {} header '{}', using default {}", H_QOS, value, defaultQos);
        return defaultQos;
    }

    private static Header header(Headers headers, String name) {
        return headers == null ? null : headers.lastHeader(name);
    }

    private static String stringHeader(Headers headers, String name, String defaultValue) {
        Header header = header(headers, name);
        return header == null || header.value() == null
            ? defaultValue
            : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
