package com.tencent.cloud.mqtt.rocketmq;

import org.apache.kafka.streams.processor.api.Record;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.cloud.mqtt.AckableRecord;

/** Ack context for {@link RocketMQSource}: acks the message (commits consumption) at the broker. */
final class RocketMQAckableRecord extends AckableRecord {
    private static final Logger log = LoggerFactory.getLogger(RocketMQAckableRecord.class);

    private final SimpleConsumer consumer;
    private final MessageView messageView;

    RocketMQAckableRecord(Record<String, String> base, SimpleConsumer consumer,
            MessageView messageView) {
        super(base);
        this.consumer = consumer;
        this.messageView = messageView;
    }

    @Override
    protected void doAck() {
        try {
            consumer.ack(messageView);
        } catch (ClientException e) {
            // A failed ack means the message becomes visible again and is
            // redelivered — safe, so just log.
            log.warn("Failed to ack message {} on topic {}: {}", messageView.getMessageId(),
                messageView.getTopic(), e.getMessage());
        }
    }
}
