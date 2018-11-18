package com.amazonaws.services.sqs;

@FunctionalInterface
public interface Deduplicated {

	public String deduplicationID();
}
