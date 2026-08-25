package com.tencent.cloud.mqtt.mqtt;

import org.apache.kafka.streams.processor.api.Record;

import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.tencent.cloud.mqtt.AckableRecord;

/** Ack context for {@link MqttSource}: acks the received publish (PUBACK) at the broker. */
final class MqttAckableRecord extends AckableRecord {

    private final Mqtt5Publish publish;

    MqttAckableRecord(Record<String, String> base, Mqtt5Publish publish) {
        super(base);
        this.publish = publish;
    }

    @Override
    protected void doAck() {
        publish.acknowledge();
    }

    public String debugString() {
        String msgId = null;
        // Inspect Message ID from user properties
        for (Mqtt5UserProperty property : publish.getUserProperties().asList()) {
            String name = property.getName().toString();
            if (MqttRecordMapper.USER_PROPERTY_MESSAGE_ID.equals(name)) {
                msgId = property.getValue().toString();
                break;
            }
        }

        return publish.getTopic().toString() + ":" + msgId;
    }
}
