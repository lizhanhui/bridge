package com.tencent.cloud.mqtt.mqtt;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.tencent.cloud.mqtt.Source;
import com.tencent.cloud.mqtt.model.Connector;

/**
 * Subscribes to an MQTT topic filter and exposes messages via {@link #poll()}.
 * The hivemq async callback feeds a bounded queue, giving natural backpressure.
 * When the queue is full, the 5s offer gives a grace window for the consumer to
 * catch up before the message is dropped (logged) — at the cost of potentially
 * stalling the client's callback thread under sustained backpressure.
 */
public class MqttSource implements Source {
    private static final Logger log = LoggerFactory.getLogger(MqttSource.class);

    private static final int QUEUE_CAPACITY = 10_000;
    private static final long OFFER_TIMEOUT_MS = 5_000;

    private final Mqtt5AsyncClient client;
    private final LinkedBlockingQueue<Record<String, String>> queue =
        new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private volatile boolean closed;

    public MqttSource(Connector connector, String topicFilter, String clientIdSuffix) {
        this.client = MqttClients.buildAsyncClient(connector, clientIdSuffix);
        client.toBlocking().connect();
        try {
            client.toAsync().subscribeWith()
                .topicFilter(topicFilter)
                .qos(MqttQos.AT_LEAST_ONCE)
                .callback(publish -> {
                    Record<String, String> record = MqttRecordMapper.toRecord(publish);
                    try {
                        if (!queue.offer(record, OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                            log.error("Source queue full for {}s, dropping message from topic {}",
                                OFFER_TIMEOUT_MS / 1000, publish.getTopic());
                        }
                    } catch (InterruptedException e) {
                        log.warn("Interrupted while enqueueing message from topic {}, dropping",
                            publish.getTopic());
                        Thread.currentThread().interrupt();
                    }
                })
                .send()
                .join();
        } catch (RuntimeException e) {
            client.toBlocking().disconnect();
            throw e;
        }
        log.info("MqttSource subscribed to {} on {}", topicFilter, connector.getAccessPoint());
    }

    @Override
    public Record<String, String> poll() throws InterruptedException {
        while (true) {
            Record<String, String> record = queue.poll(500, TimeUnit.MILLISECONDS);
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
