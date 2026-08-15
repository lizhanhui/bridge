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
 * On publish, {@value #BROKER_PROPERTY_PREFIX}-prefixed headers are dropped —
 * they are broker-assigned and the broker sets fresh ones on delivery.
 */
public final class MqttRecordMapper {
    private static final Logger log = LoggerFactory.getLogger(MqttRecordMapper.class);

    public static final String H_TOPIC = "mqtt.topic";
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
        H_TOPIC, H_QOS, H_RETAINED, H_DUPLICATE, H_PACKET_ID,
        H_CONTENT_TYPE, H_CORRELATION_DATA, H_RESPONSE_TOPIC, H_MESSAGE_EXPIRY_INTERVAL);

    private MqttRecordMapper() {
    }

    public static Record<String, String> toRecord(Mqtt5Publish publish) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(H_TOPIC, utf8(publish.getTopic().toString()));
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
            String value = property.getValue().toString();
            headers.add(name, utf8(value));
            if (USER_PROPERTY_MESSAGE_ID.equals(name)) {
                key = value;
            }
        }

        String value = new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8);
        return new Record<>(key, value, System.currentTimeMillis(), headers);
    }

    public static Mqtt5Publish toPublish(Record<String, String> record, String defaultTopic,
            MqttQos defaultQos) {
        Headers headers = record.headers();

        Mqtt5PublishBuilder.Complete builder = Mqtt5Publish.builder()
            .topic(stringHeader(headers, H_TOPIC, defaultTopic))
            .qos(qosHeader(headers, defaultQos))
            .retain("true".equals(stringHeader(headers, H_RETAINED, null)))
            .payload(record.value().getBytes(StandardCharsets.UTF_8));

        String contentType = stringHeader(headers, H_CONTENT_TYPE, null);
        if (contentType != null) {
            builder.contentType(contentType);
        }
        Header correlationData = header(headers, H_CORRELATION_DATA);
        if (correlationData != null) {
            builder.correlationData(ByteBuffer.wrap(correlationData.value()));
        }
        String responseTopic = stringHeader(headers, H_RESPONSE_TOPIC, null);
        if (responseTopic != null) {
            builder.responseTopic(responseTopic);
        }
        String expiry = stringHeader(headers, H_MESSAGE_EXPIRY_INTERVAL, null);
        if (expiry != null) {
            try {
                builder.messageExpiryInterval(Long.parseLong(expiry));
            } catch (NumberFormatException e) {
                log.warn("Malformed {} header '{}', skipping", H_MESSAGE_EXPIRY_INTERVAL, expiry);
            }
        }

        if (headers != null) {
            Mqtt5UserPropertiesBuilder propsBuilder = Mqtt5UserProperties.builder();
            boolean hasProps = false;
            for (Header header : headers) {
                String name = header.key();
                if (!RESERVED_HEADERS.contains(name) && !name.startsWith(BROKER_PROPERTY_PREFIX)) {
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
        return header == null ? defaultValue : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
