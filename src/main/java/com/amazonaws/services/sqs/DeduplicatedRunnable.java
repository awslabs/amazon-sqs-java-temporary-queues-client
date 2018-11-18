package com.amazonaws.services.sqs;

public class DeduplicatedRunnable implements SerializableRunnable, Deduplicated {

	private static final long serialVersionUID = -2531422031653089835L;
	
	private final SerializableRunnable task;
	private final String deduplicationId;
	
	public DeduplicatedRunnable(SerializableRunnable task) {
		this(task, null);
	}
	
	public DeduplicatedRunnable(SerializableRunnable task, String deduplicationId) {
		this.task = task;
		this.deduplicationId = deduplicationId;
	}
	
	public static DeduplicatedRunnable deduplicated(SerializableRunnable task) {
		return new DeduplicatedRunnable(task);
	}
	
	public static DeduplicatedRunnable deduplicated(SerializableRunnable task, String deduplicationId) {
		return new DeduplicatedRunnable(task, deduplicationId);
	}
	
	@Override
	public void run() {
		task.run();
	}
	
	@Override
	public String deduplicationID() {
		return deduplicationId;
	}
}
