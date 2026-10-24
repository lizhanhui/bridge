package com.tencent.cloud.mqtt.kafka;

import org.apache.kafka.streams.processor.api.Record;
import com.tencent.cloud.mqtt.AckableRecord;

public final class KafkaAckableRecord extends AckableRecord {
    private final KafkaSource source;
    private final KafkaSource.CompletionToken completion;

    KafkaAckableRecord(Record<String, String> record, KafkaSource source, KafkaSource.CompletionToken completion) {
        super(record);
        this.source = source;
        this.completion = completion;
    }

    @Override
    protected void doAck() { source.complete(completion); }

    @Override
    protected String debugString() {
        return completion.partition.topic() + ":" + completion.partition.partition() + ":" + completion.record.offset();
    }
}
