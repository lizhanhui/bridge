package com.tencent.cloud.mqtt.kafka;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.streams.processor.api.Record;

import com.tencent.cloud.mqtt.Sink;
import com.tencent.cloud.mqtt.model.Connector;

public class KafkaSink implements Sink {
    private final Producer<String, String> producer;
    private final String topic;

    public KafkaSink(Connector connector, String topic, String clientId) {
        KafkaRecordMapper.validateTopic(topic);
        this.topic = topic;
        this.producer = KafkaClients.createProducer(connector, clientId);
    }

    KafkaSink(Producer<String, String> producer, String topic) {
        KafkaRecordMapper.validateTopic(topic);
        this.topic = topic;
        this.producer = producer;
    }

    @Override
    public void publish(Record<String, String> record) {
        try {
            producer.send(KafkaRecordMapper.toProducerRecord(record, topic)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptException(e);
        } catch (ExecutionException e) {
            throw publicationFailure(e.getCause());
        } catch (RuntimeException e) {
            throw publicationFailure(e);
        }
    }

    private static RuntimeException publicationFailure(Throwable failure) {
        if (failure instanceof InterruptException || failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return failure instanceof InterruptException interrupted ? interrupted
                : new InterruptException((InterruptedException) failure);
        }
        if (failure instanceof RecordTooLargeException || failure instanceof InvalidTopicException
                || failure instanceof InvalidTimestampException || failure instanceof SerializationException) {
            return new IllegalArgumentException("Kafka rejected the record", failure);
        }
        return failure instanceof RuntimeException runtime ? runtime
            : new KafkaException("Kafka publication failed", failure);
    }

    @Override
    public void close() {
        boolean interrupted = Thread.currentThread().isInterrupted();
        try {
            producer.close(Duration.ofSeconds(10));
        } catch (InterruptException e) {
            // Closing is best-effort during task shutdown; preserve the primary interruption.
            interrupted = true;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
