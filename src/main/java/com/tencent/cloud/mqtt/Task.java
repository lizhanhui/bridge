package com.tencent.cloud.mqtt;

public class Task {
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

    public void launch() {

    }

}
