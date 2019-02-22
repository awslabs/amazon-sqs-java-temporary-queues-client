package com.amazonaws.services.sqs.executors;

import java.io.Serializable;
import java.util.function.Function;

public interface SerializableFunction<T, R> extends Serializable, Function<T, R> {
    public static <T, R> SerializableFunction<T, R> serializable(SerializableFunction<T, R> function) {
        return function;
    }
}
