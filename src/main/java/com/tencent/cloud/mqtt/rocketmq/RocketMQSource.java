package com.tencent.cloud.mqtt.rocketmq;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.cloud.mqtt.AckableRecord;
import com.tencent.cloud.mqtt.Source;
import com.tencent.cloud.mqtt.model.Connector;

/**
 * Consumes RocketMQ messages via a {@link SimpleConsumer} and exposes them via
 * {@link #poll()}.
 *
 * <p>Crash safety: the broker is acked only when {@link AckableRecord#ack()}
 * is invoked by the consumer after processing completes. A message that is
 * never acked (task crash, shutdown, sink failure) becomes visible again when
 * its invisible duration expires and is redelivered.
 *
 * <p>Messages are received one per RPC (no local queue) so the invisible
 * duration always covers exactly the message being processed — a batch
 * waiting in a local queue could expire before its turn, causing premature
 * redelivery and ack-after-expiry noise. Throughput scales via task lanes,
 * which load-balance within the shared consumer group.
 */
public class RocketMQSource implements Source {
    private static final Logger log = LoggerFactory.getLogger(RocketMQSource.class);

    /** Must cover one message's transform + publish. */
    private static final Duration INVISIBLE_DURATION = Duration.ofSeconds(30);

    private final SimpleConsumer consumer;
    private final String consumerGroup;
    private volatile boolean closed;

    public RocketMQSource(Connector connector, String consumerGroup, List<String> topics) {
        this.consumer = RocketMQClients.buildSimpleConsumer(connector, consumerGroup, topics);
        this.consumerGroup = consumerGroup;
        log.info("RocketMQSource consuming {} in group {} on {}", topics, consumerGroup,
            connector.getAccessPoint());
    }

    @Override
    public AckableRecord poll() throws InterruptedException {
        while (true) {
            if (closed) {
                return null;
            }
            List<MessageView> messages;
            try {
                messages = consumer.receive(1, INVISIBLE_DURATION);
            } catch (ClientException e) {
                if (closed || Thread.currentThread().isInterrupted()) {
                    return null;
                }
                // fail-fast, consistent with the sink side: the lane dies and
                // unacked messages are redelivered
                throw new RuntimeException("Receive failed for group " + consumerGroup, e);
            }
            if (messages.isEmpty()) {
                continue;
            }
            MessageView messageView = messages.get(0);
            return new RocketMQAckableRecord(
                RocketMQRecordMapper.toRecord(messageView), consumer, messageView);
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            consumer.close();
        } catch (IOException e) {
            log.warn("Error closing SimpleConsumer for group {}", consumerGroup, e);
        }
    }
}
