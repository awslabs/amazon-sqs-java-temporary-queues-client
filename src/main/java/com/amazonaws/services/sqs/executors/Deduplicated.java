package com.amazonaws.services.sqs.executors;

@FunctionalInterface
public interface Deduplicated {

	public String deduplicationID();
}
