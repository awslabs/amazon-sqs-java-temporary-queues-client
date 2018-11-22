package com.amazonaws.services.sqs.executors;

import java.io.Serializable;

public interface SerializableRunnable extends Serializable, Runnable {
	
	public static SerializableRunnable serializable(SerializableRunnable runnable) {
		return runnable;
	}
	
	public static <T> SerializableCallable<T> serializable(SerializableCallable<T> runnable) {
		return runnable;
	}
}
