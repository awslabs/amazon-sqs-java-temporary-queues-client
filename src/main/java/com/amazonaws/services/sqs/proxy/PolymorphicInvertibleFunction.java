package com.amazonaws.services.sqs.proxy;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PolymorphicInvertibleFunction<T, R> implements BiFunction<Class<? extends T>, T, R> {

    private final Map<Class<? extends T>, InvertibleFunction<T, R>> cases = new HashMap<>();
    
    public <U extends T> void addCase(Class<U> klass, InvertibleFunction<U, R> serializer) {
        cases.put(klass, serializer.compose(InvertibleFunction.cast(klass)));
    }
    
    public <U extends T> void addCase(Class<U> klass, Function<U, R> serializer, Function<R, U> deserializer) {
        addCase(klass, InvertibleFunction.of(serializer, deserializer));
    }

    @SuppressWarnings("unchecked")
    public <U extends T> InvertibleFunction<U, R> forClass(Class<U> type) {
        return (InvertibleFunction<U, R>)cases.get(type);
    }

    @SuppressWarnings("unchecked")
    public InvertibleFunction<T, R> restricted(Class<? extends T> type) {
        return cases.get(type);
    }

    @Override
    public R apply(Class<? extends T> type, T t) {
        return cases.get(type).apply(t);
    }
}
