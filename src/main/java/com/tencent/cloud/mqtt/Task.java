package com.tencent.cloud.mqtt;

import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
    private static final Logger log = LoggerFactory.getLogger(Task.class);

    private final String name;

    private final Source source;

    private final Transform<String, String> transform;

    private final Sink sink;

    public Task(String name, Source source, Transform<String, String> transform, Sink sink) {
        this.name = name;
        this.source = source;
        this.transform = transform;
        this.sink = sink;
    }

    public String getName() {
        return name;
    }

    public void launch() throws InterruptedException {
        log.info("Task {} started", name);
        try {
            Record<String, String> record;
            while ((record = source.poll()) != null) {
                transform.transform(record)
                    .ifPresent(records -> records.forEach(sink::publish));
            }
        } finally {
            try {
                source.close();
            } finally {
                sink.close();
            }
            log.info("Task {} stopped", name);
        }
    }
}
