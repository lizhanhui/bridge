package com.tencent.cloud.mqtt.model;

import com.tencent.cloud.mqtt.Sink;

public class RocketMQSink implements Sink {
    private final Connector connector;

    private final String topic;

    public RocketMQSink(Connector connector, String topic) {
        this.connector = connector;
        this.topic = topic;
    }

    public Connector getConnector() {
        return connector;
    }

    public String getTopic() {
        return topic;
    }
}
