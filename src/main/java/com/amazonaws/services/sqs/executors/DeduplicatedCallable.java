package com.amazonaws.services.sqs.executors;

public class DeduplicatedCallable<V> implements SerializableCallable<V>, Deduplicated {

    private static final long serialVersionUID = -2531422031653089835L;

    private final SerializableCallable<V> task;
    private final String deduplicationId;

    public DeduplicatedCallable(SerializableCallable<V> task) {
        this(task, null);
    }

    public DeduplicatedCallable(SerializableCallable<V> task, String deduplicationId) {
        this.task = task;
        this.deduplicationId = deduplicationId;
    }

    public static <V> DeduplicatedCallable<V> deduplicated(SerializableCallable<V> task) {
        return new DeduplicatedCallable<>(task);
    }

    public static <V> DeduplicatedCallable<V> deduplicated(SerializableCallable<V> task, String deduplicationId) {
        return new DeduplicatedCallable<>(task, deduplicationId);
    }

    @Override
    public V call() throws Exception {
        return task.call();
    }

    @Override
    public String deduplicationID() {
        return deduplicationId;
    }
}
