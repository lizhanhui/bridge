package com.tencent.cloud.mqtt.rocketmq;

import java.io.IOException;

import org.apache.kafka.streams.processor.api.Record;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.cloud.mqtt.Sink;
import com.tencent.cloud.mqtt.model.Connector;

/** Publishes records to a fixed RocketMQ topic. Fail-fast: send errors propagate and kill the task. */
public class RocketMQSink implements Sink {
    private static final Logger log = LoggerFactory.getLogger(RocketMQSink.class);

    private final Producer producer;
    private final String topic;
    private final String accessPoint;

    public RocketMQSink(Connector connector, String topic) {
        this.producer = RocketMQClients.buildProducer(connector, topic);
        this.topic = topic;
        this.accessPoint = connector.getAccessPoint();
        log.info("RocketMQSink connected to {} for topic {}", accessPoint, topic);
    }

    @Override
    public void publish(Record<String, String> record) {
        try {
            producer.send(RocketMQRecordMapper.toMessage(
                ClientServiceProvider.loadService(), record, topic));
        } catch (ClientException e) {
            throw new RuntimeException(
                "Failed to publish to topic " + topic + " on " + accessPoint, e);
        }
    }

    @Override
    public void close() {
        try {
            producer.close();
        } catch (IOException e) {
            log.warn("Error closing Producer for topic {}", topic, e);
        }
    }
}
