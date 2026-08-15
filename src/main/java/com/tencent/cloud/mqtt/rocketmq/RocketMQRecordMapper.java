package com.tencent.cloud.mqtt.rocketmq;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps between RocketMQ messages and bridge records. Protocol info travels in
 * {@code rmq.*} headers; message properties round-trip as headers with the
 * same name. The record key is the broker-assigned message ID (symmetric with
 * the MQTT side, whose {@code $__messageId} user property is the broker's
 * UNIQ_KEY). On produce, {@value #BROKER_PROPERTY_PREFIX}-prefixed headers are
 * dropped — they are broker-assigned MQTT properties.
 */
public final class RocketMQRecordMapper {
    private static final Logger log = LoggerFactory.getLogger(RocketMQRecordMapper.class);

    public static final String H_TOPIC = "rmq.topic";
    public static final String H_MESSAGE_ID = "rmq.message.id";
    public static final String H_TAG = "rmq.tag";
    public static final String H_KEYS = "rmq.keys";
    public static final String H_BORN_TIMESTAMP = "rmq.born.timestamp";
    public static final String H_DELIVERY_TIMESTAMP = "rmq.delivery.timestamp";
    public static final String H_DELIVERY_ATTEMPT = "rmq.delivery.attempt";
    public static final String H_BORN_HOST = "rmq.born.host";

    /** Broker-assigned MQTT user-property prefix; such headers are dropped on produce. */
    public static final String BROKER_PROPERTY_PREFIX = "$__";

    private static final Set<String> RESERVED_HEADERS = Set.of(
        H_TOPIC, H_MESSAGE_ID, H_TAG, H_KEYS, H_BORN_TIMESTAMP,
        H_DELIVERY_TIMESTAMP, H_DELIVERY_ATTEMPT, H_BORN_HOST);

    private RocketMQRecordMapper() {
    }

    /**
     * Maps a received message to a record. The key is the broker message ID.
     * Properties whose names collide with reserved {@code rmq.*} headers are
     * skipped (with a warning) so they cannot shadow the real protocol headers.
     * Timestamp is the message born timestamp.
     */
    public static Record<String, String> toRecord(MessageView messageView) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(H_TOPIC, utf8(messageView.getTopic()));
        String messageId = messageView.getMessageId().toString();
        headers.add(H_MESSAGE_ID, utf8(messageId));
        messageView.getTag().ifPresent(tag -> headers.add(H_TAG, utf8(tag)));
        if (!messageView.getKeys().isEmpty()) {
            headers.add(H_KEYS, utf8(String.join(",", messageView.getKeys())));
        }
        headers.add(H_BORN_TIMESTAMP, utf8(Long.toString(messageView.getBornTimestamp())));
        messageView.getDeliveryTimestamp().ifPresent(
            ts -> headers.add(H_DELIVERY_TIMESTAMP, utf8(Long.toString(ts))));
        headers.add(H_DELIVERY_ATTEMPT, utf8(Integer.toString(messageView.getDeliveryAttempt())));
        headers.add(H_BORN_HOST, utf8(messageView.getBornHost()));

        for (Map.Entry<String, String> property : messageView.getProperties().entrySet()) {
            if (RESERVED_HEADERS.contains(property.getKey())) {
                log.warn("Skipping property '{}': reserved record header name", property.getKey());
                continue;
            }
            headers.add(property.getKey(), utf8(property.getValue()));
        }

        ByteBuffer body = messageView.getBody();
        byte[] bytes = new byte[body.remaining()];
        body.get(bytes);
        return new Record<>(messageId, new String(bytes, StandardCharsets.UTF_8),
            messageView.getBornTimestamp(), headers);
    }

    /**
     * Maps a record to an outgoing message. The {@code rmq.topic} header
     * overrides {@code defaultTopic}; {@code rmq.tag} / {@code rmq.keys}
     * (comma-separated) set the message tag/keys. Reserved {@code rmq.*}
     * headers are consumed as message settings and
     * {@value #BROKER_PROPERTY_PREFIX}-prefixed headers are dropped; every
     * other header becomes a message property.
     */
    public static Message toMessage(ClientServiceProvider provider, Record<String, String> record,
            String defaultTopic) {
        Headers headers = record.headers();

        MessageBuilder builder = provider.newMessageBuilder()
            .setTopic(stringHeader(headers, H_TOPIC, defaultTopic))
            .setBody(record.value().getBytes(StandardCharsets.UTF_8));

        String tag = stringHeader(headers, H_TAG, null);
        if (tag != null) {
            builder.setTag(tag);
        }
        String keys = stringHeader(headers, H_KEYS, null);
        if (keys != null) {
            builder.setKeys(keys.split(","));
        }

        if (headers != null) {
            for (Header header : headers) {
                String name = header.key();
                if (!RESERVED_HEADERS.contains(name) && !name.startsWith(BROKER_PROPERTY_PREFIX)
                        && header.value() != null) {
                    builder.addProperty(name, new String(header.value(), StandardCharsets.UTF_8));
                }
            }
        }

        return builder.build();
    }

    private static String stringHeader(Headers headers, String name, String defaultValue) {
        if (headers == null) {
            return defaultValue;
        }
        Header header = headers.lastHeader(name);
        return header == null || header.value() == null
            ? defaultValue
            : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
