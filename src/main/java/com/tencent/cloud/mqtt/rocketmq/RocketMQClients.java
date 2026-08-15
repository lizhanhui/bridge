package com.tencent.cloud.mqtt.rocketmq;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.producer.Producer;

import com.tencent.cloud.mqtt.model.Connector;

/** Builds RocketMQ 5.x proxy-protocol clients from connector config. */
final class RocketMQClients {

    /** Long-poll window for SimpleConsumer.receive; also bounds shutdown latency. */
    static final Duration AWAIT_DURATION = Duration.ofSeconds(3);

    private RocketMQClients() {}

    private static ClientConfiguration clientConfiguration(Connector connector) {
        var builder = ClientConfiguration.newBuilder()
            .setEndpoints(connector.getAccessPoint());
        String username = connector.getUsername();
        if (username != null && !username.isBlank()) {
            builder.setCredentialProvider(
                new StaticSessionCredentialsProvider(username, connector.getPassword()));
        }
        return builder.build();
    }

    static SimpleConsumer buildSimpleConsumer(Connector connector, String consumerGroup,
            List<String> topics) {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        Map<String, FilterExpression> subscriptions = new HashMap<>();
        for (String topic : topics) {
            subscriptions.put(topic, FilterExpression.SUB_ALL);
        }
        try {
            return provider.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfiguration(connector))
                .setConsumerGroup(consumerGroup)
                .setSubscriptionExpressions(subscriptions)
                .setAwaitDuration(AWAIT_DURATION)
                .build();
        } catch (org.apache.rocketmq.client.apis.ClientException e) {
            throw new RuntimeException("Failed to build SimpleConsumer for group "
                + consumerGroup, e);
        }
    }

    static Producer buildProducer(Connector connector, String topic) {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        try {
            return provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration(connector))
                .setTopics(topic)
                .build();
        } catch (org.apache.rocketmq.client.apis.ClientException e) {
            throw new RuntimeException("Failed to build Producer for topic " + topic, e);
        }
    }
}
