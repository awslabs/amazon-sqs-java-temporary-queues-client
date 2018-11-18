package com.amazonaws.services.sqs;

import java.io.Serializable;
import java.util.concurrent.Callable;

public interface SerializableCallable<V> extends Serializable, Callable<V> {
	
	public static <V> SerializableCallable<V> serializable(SerializableCallable<V> callable) {
		return callable;
	}
}
