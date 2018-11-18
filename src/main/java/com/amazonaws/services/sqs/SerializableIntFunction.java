package com.amazonaws.services.sqs;

import java.io.Serializable;
import java.util.function.IntFunction;

public interface SerializableIntFunction<R> extends Serializable, IntFunction<R> {
	public static <R> SerializableIntFunction<R> serializable(SerializableIntFunction<R> function) {
		return function;
	}
}
