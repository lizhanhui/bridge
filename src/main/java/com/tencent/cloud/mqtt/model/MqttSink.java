package com.tencent.cloud.mqtt.model;

import com.tencent.cloud.mqtt.Sink;

public class MqttSink implements Sink {
    private final Connector connector;
    private final String topic;

    public MqttSink(Connector connector, String topic) {
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
