package com.tencent.cloud.mqtt.model;

import com.tencent.cloud.mqtt.Source;
import java.util.List;

public class RocketMQSource implements Source {
    private final Connector connector;
    private final String consumerGroup;
    private final List<String> topics;
    public RocketMQSource(Connector connector, String consumerGroup, List<String> topics) {
        this.connector = connector;
        this.consumerGroup = consumerGroup;
        this.topics = topics;
    }

    public Connector getConnector() {
        return connector;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public List<String> getTopics() {
        return topics;
    }
}
