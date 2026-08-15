package com.tencent.cloud.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
    private static final Logger log = LoggerFactory.getLogger(Task.class);

    private final String name;

    private final Source source;

    private final Transform transform;

    private final Sink sink;

    public Task(String name, Source source, Transform transform, Sink sink) {
        this.name = name;
        this.source = source;
        this.transform = transform;
        this.sink = sink;
    }

    public String getName() {
        return name;
    }

    public void launch() throws InterruptedException {
        // TODO: wire source -> transform -> sink message flow
        log.info("Task {} started", name);
        Thread.sleep(Long.MAX_VALUE);
    }

}
