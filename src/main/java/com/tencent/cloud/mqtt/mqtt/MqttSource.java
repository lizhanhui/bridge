package com.tencent.cloud.mqtt.mqtt;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.tencent.cloud.mqtt.AckableRecord;
import com.tencent.cloud.mqtt.Source;
import com.tencent.cloud.mqtt.model.Connector;

/**
 * Subscribes to an MQTT topic filter and exposes messages via {@link #poll()}.
 * The hivemq async callback feeds a bounded queue, giving natural backpressure.
 *
 * <p>Crash safety: the subscription uses manual acknowledgement — the broker
 * is acked only when {@link AckableRecord#ack()} is invoked by the consumer
 * after processing completes. Records that are never acked (task crash,
 * queue-full timeout, shutdown) stay unacknowledged and are redelivered by
 * the broker. The client connects with a persistent session
 * ({@code cleanStart=false}, 24h expiry) so this redelivery also works for
 * non-shared subscriptions; shared subscriptions redeliver to the share
 * group regardless.
 *
 * <p>When the queue is full, the 5s offer gives a grace window for the
 * consumer to catch up; on timeout the message is left unacked (it will be
 * redelivered after reconnect) — at the cost of potentially stalling the
 * client's callback thread under sustained backpressure.
 */
public class MqttSource implements Source {
    private static final Logger log = LoggerFactory.getLogger(MqttSource.class);

    private static final int QUEUE_CAPACITY = 10_000;
    private static final long OFFER_TIMEOUT_MS = 5_000;
    private static final long SESSION_EXPIRY_SECONDS = 86_400;

    private final Mqtt5AsyncClient client;
    private final LinkedBlockingQueue<AckableRecord> queue =
        new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private volatile boolean closed;

    public MqttSource(Connector connector, String topicFilter, String clientIdSuffix) {
        this.client = MqttClients.buildAsyncClient(connector, clientIdSuffix);
        client.connectWith()
            .cleanStart(false)
            .sessionExpiryInterval(SESSION_EXPIRY_SECONDS)
            .send()
            .join();
        try {
            client.toAsync().subscribeWith()
                .topicFilter(topicFilter)
                .qos(MqttQos.AT_LEAST_ONCE)
                .callback(publish -> {
                    AckableRecord record =
                        new MqttAckableRecord(MqttRecordMapper.toRecord(publish), publish);
                    try {
                        if (!queue.offer(record, OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                            log.error("Source queue full for {}s, leaving message from topic {} "
                                + "unacked (will be redelivered after reconnect)",
                                OFFER_TIMEOUT_MS / 1000, publish.getTopic());
                        }
                    } catch (InterruptedException e) {
                        log.warn("Interrupted while enqueueing message from topic {}, "
                            + "leaving it unacked (will be redelivered)", publish.getTopic());
                        Thread.currentThread().interrupt();
                    }
                })
                .manualAcknowledgement(true)
                .send()
                .join();
        } catch (RuntimeException e) {
            client.toBlocking().disconnect();
            throw e;
        }
        log.info("MqttSource subscribed to {} on {}", topicFilter, connector.getAccessPoint());
    }

    @Override
    public AckableRecord poll() throws InterruptedException {
        while (true) {
            AckableRecord record = queue.poll(500, TimeUnit.MILLISECONDS);
            if (record != null) {
                return record;
            }
            if (closed && queue.isEmpty()) {
                return null;
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        client.toBlocking().disconnect();
    }
}
