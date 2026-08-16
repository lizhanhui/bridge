package com.tencent.cloud.mqtt.rocketmq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.streams.processor.api.Record;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.junit.jupiter.api.Test;

import com.tencent.cloud.mqtt.Task;

class RocketMQRecordMapperTest {

    /** Minimal offline MessageView stub; only the getters the mapper reads are meaningful. */
    record StubMessageId(String id) implements MessageId {
        @Override public String getVersion() { return "stub"; }
        @Override public String toString() { return id; }
    }

    static class StubMessageView implements MessageView {
        private final String topic;
        private final String messageId;
        private final ByteBuffer body;
        private final Map<String, String> properties;
        private final String tag;
        private final List<String> keys;
        private final long bornTimestamp;

        StubMessageView(String topic, String messageId, String body, Map<String, String> properties,
                String tag, List<String> keys, long bornTimestamp) {
            this.topic = topic;
            this.messageId = messageId;
            this.body = ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8));
            this.properties = properties;
            this.tag = tag;
            this.keys = keys;
            this.bornTimestamp = bornTimestamp;
        }

        @Override public MessageId getMessageId() { return new StubMessageId(messageId); }
        @Override public String getTopic() { return topic; }
        @Override public ByteBuffer getBody() { return body.duplicate(); }
        @Override public Map<String, String> getProperties() { return properties; }
        @Override public Optional<String> getTag() { return Optional.ofNullable(tag); }
        @Override public Collection<String> getKeys() { return keys; }
        @Override public Optional<String> getMessageGroup() { return Optional.empty(); }
        @Override public Optional<String> getLiteTopic() { return Optional.empty(); }
        @Override public Optional<Long> getDeliveryTimestamp() { return Optional.empty(); }
        @Override public Optional<Integer> getPriority() { return Optional.empty(); }
        @Override public String getBornHost() { return "127.0.0.1"; }
        @Override public long getBornTimestamp() { return bornTimestamp; }
        @Override public int getDeliveryAttempt() { return 1; }
    }

    private static String header(Record<String, String> record, String name) {
        var h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    @Test
    void messageViewMapsToRecord() {
        StubMessageView view = new StubMessageView("home", "msg-1", "{\"age\":20}",
            Map.of("color", "red", Task.HOP_COUNT_HEADER, "1"), "tagA", List.of("k1", "k2"), 1234L);

        Record<String, String> record = RocketMQRecordMapper.toRecord(view);

        assertEquals("msg-1", record.key());
        assertEquals("{\"age\":20}", record.value());
        assertEquals(1234L, record.timestamp());
        assertEquals("home", header(record, RocketMQRecordMapper.H_SRC_TOPIC));
        assertEquals("msg-1", header(record, RocketMQRecordMapper.H_MESSAGE_ID));
        assertEquals("tagA", header(record, RocketMQRecordMapper.H_TAG));
        assertEquals("k1,k2", header(record, RocketMQRecordMapper.H_KEYS));
        assertEquals("1234", header(record, RocketMQRecordMapper.H_BORN_TIMESTAMP));
        assertEquals("1", header(record, RocketMQRecordMapper.H_DELIVERY_ATTEMPT));
        // plain properties round-trip as same-name headers
        assertEquals("red", header(record, "color"));
        assertEquals("1", header(record, Task.HOP_COUNT_HEADER));
    }

    @Test
    void propertyCollidingWithReservedHeaderIsSkipped() {
        StubMessageView view = new StubMessageView("home", "msg-1", "{}",
            Map.of("src.rmq.topic", "evil"), null, List.of(), 0L);

        Record<String, String> record = RocketMQRecordMapper.toRecord(view);

        assertEquals("home", header(record, RocketMQRecordMapper.H_SRC_TOPIC));
    }

    @Test
    void recordMapsToMessage() {
        Record<String, String> record = new Record<>("k", "{\"age\":20}", 1L);
        record.headers().add("color", "red".getBytes(StandardCharsets.UTF_8));
        record.headers().add(Task.HOP_COUNT_HEADER, "2".getBytes(StandardCharsets.UTF_8));
        record.headers().add(RocketMQRecordMapper.H_TAG, "tagA".getBytes(StandardCharsets.UTF_8));
        record.headers().add(RocketMQRecordMapper.H_KEYS, "k1,k2".getBytes(StandardCharsets.UTF_8));

        Message message = RocketMQRecordMapper.toMessage(
            ClientServiceProvider.loadService(), record, "home");

        assertEquals("home", message.getTopic());
        assertEquals("{\"age\":20}",
            StandardCharsets.UTF_8.decode(message.getBody()).toString());
        assertEquals(Optional.of("tagA"), message.getTag());
        assertEquals(List.of("k1", "k2"), List.copyOf(message.getKeys()));
        assertEquals(Map.of("color", "red", Task.HOP_COUNT_HEADER, "2"), message.getProperties());
    }

    @Test
    void dstTopicHeaderOverridesDefaultTopic() {
        Record<String, String> record = new Record<>("k", "{}", 1L);
        record.headers().add(RocketMQRecordMapper.H_DST_TOPIC,
            "other".getBytes(StandardCharsets.UTF_8));

        Message message = RocketMQRecordMapper.toMessage(
            ClientServiceProvider.loadService(), record, "home");

        assertEquals("other", message.getTopic());
        assertTrue(message.getProperties().isEmpty());
    }

    @Test
    void srcTopicHeaderPropagatesAsProperty() {
        Record<String, String> record = new Record<>("k", "{}", 1L);
        record.headers().add(RocketMQRecordMapper.H_SRC_TOPIC,
            "home/in".getBytes(StandardCharsets.UTF_8));

        Message message = RocketMQRecordMapper.toMessage(
            ClientServiceProvider.loadService(), record, "home");

        assertEquals("home", message.getTopic());
        assertEquals(Map.of("src.rmq.topic", "home/in"), message.getProperties());
    }

    @Test
    void dstTopicPropertyFromProducerBecomesRoutingHeader() {
        StubMessageView view = new StubMessageView("home", "msg-1", "{}",
            Map.of("dst.rmq.topic", "reroute"), null, List.of(), 0L);

        Record<String, String> record = RocketMQRecordMapper.toRecord(view);

        assertEquals("reroute", header(record, RocketMQRecordMapper.H_DST_TOPIC));
        // and it survives to drive the sink topic on produce:
        Message message = RocketMQRecordMapper.toMessage(
            ClientServiceProvider.loadService(), record, "home");
        assertEquals("reroute", message.getTopic());
    }

    @Test
    void reservedAndBrokerPrefixedHeadersAreDroppedOnProduce() {
        Record<String, String> record = new Record<>("k", "{}", 1L);
        record.headers().add(RocketMQRecordMapper.H_MESSAGE_ID,
            "msg-9".getBytes(StandardCharsets.UTF_8));
        record.headers().add(RocketMQRecordMapper.H_BORN_HOST,
            "10.0.0.1".getBytes(StandardCharsets.UTF_8));
        record.headers().add("$__messageId", "abc".getBytes(StandardCharsets.UTF_8));
        record.headers().add("color", "red".getBytes(StandardCharsets.UTF_8));

        Message message = RocketMQRecordMapper.toMessage(
            ClientServiceProvider.loadService(), record, "home");

        assertEquals(Map.of("color", "red"), message.getProperties());
    }

    @Test
    void roundTripPreservesProperties() {
        StubMessageView view = new StubMessageView("home", "msg-1", "payload",
            Map.of("color", "red", Task.HOP_COUNT_HEADER, "1"), null, List.of(), 100L);

        Record<String, String> record = RocketMQRecordMapper.toRecord(view);
        Message message = RocketMQRecordMapper.toMessage(
            ClientServiceProvider.loadService(), record, "ignored");

        // src.rmq.topic propagates as a property; it no longer overrides the topic
        assertEquals("ignored", message.getTopic());
        assertEquals(Map.of("color", "red", Task.HOP_COUNT_HEADER, "1",
            "src.rmq.topic", "home"), message.getProperties());
        byte[] body = new byte[message.getBody().remaining()];
        message.getBody().get(body);
        assertArrayEquals("payload".getBytes(StandardCharsets.UTF_8), body);
    }
}
