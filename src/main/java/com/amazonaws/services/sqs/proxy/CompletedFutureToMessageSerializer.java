package com.amazonaws.services.sqs.proxy;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CompletedFutureToMessageSerializer<T> implements InvertibleFunction<Future<T>, String> {

    private static final char NORMAL_VALUE_PREFIX = '.';
    private static final char CANCELLED_PREFIX = 'C';
    private static final char EXCEPTION_PREFIX = 'E';
    
    private final InvertibleFunction<T, String> resultSerializer;
    private final InvertibleFunction<Throwable, String> exceptionSerializer;
    
    public CompletedFutureToMessageSerializer(InvertibleFunction<Object, String> serializer) {
        this(serializer.compose(InvertibleFunction.uncheckedCast()),
             serializer.compose(InvertibleFunction.uncheckedCast()));
    }
    
    public CompletedFutureToMessageSerializer(InvertibleFunction<T, String> resultSerializer,
                                              InvertibleFunction<Throwable, String> exceptionSerializer) {
        this.resultSerializer = resultSerializer;
        this.exceptionSerializer = exceptionSerializer;
    }
    
    @Override
    public String apply(Future<T> future) {
        if (!future.isDone()) {
            throw new IllegalArgumentException();
        }
        try {
            return NORMAL_VALUE_PREFIX + resultSerializer.apply(future.get());
        } catch (InterruptedException e) {
            // Should never happen
            throw new IllegalStateException();
        } catch (ExecutionException e) {
            return EXCEPTION_PREFIX + exceptionSerializer.apply(e.getCause());
        } catch (CancellationException e) {
            return Character.toString(CANCELLED_PREFIX);
        }
    }
    
    @Override
    public Future<T> unapply(String serialized) {
        CompletableFuture<T> future = new CompletableFuture<>();
        String serializedValue = serialized.substring(1, serialized.length());
        switch (serialized.charAt(0)) {
        case NORMAL_VALUE_PREFIX:
            future.complete(resultSerializer.unapply(serializedValue));
            break;
        case CANCELLED_PREFIX:
            future.cancel(false);
            break;
        case EXCEPTION_PREFIX:
            future.completeExceptionally(exceptionSerializer.unapply(serializedValue));
            break;
        }
        return future;
    }

}
