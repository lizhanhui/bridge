package com.tencent.cloud.mqtt.model;

import com.tencent.cloud.mqtt.Source;

public class MqttSource implements Source {
    private final Connector connector;

    private final String topicFilter;

    public MqttSource(Connector connector, String topicFilter) {
        this.connector = connector;
        this.topicFilter = topicFilter;
    }

    public Connector getConnector() {
        return connector;
    }

    public String getTopicFilter() {
        return topicFilter;
    }
}
