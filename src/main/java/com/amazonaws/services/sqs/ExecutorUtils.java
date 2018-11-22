package com.amazonaws.services.sqs;

import java.io.Serializable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;

public class ExecutorUtils {
    
	private static class FutureGetter<V> implements ForkJoinPool.ManagedBlocker {

    	private final Future<V> future;
    	
    	public FutureGetter(Future<V> future) {
    		this.future = future;
    	}
    	
		@Override
		public boolean block() throws InterruptedException {
			try {
				future.get();
            } catch (CancellationException|ExecutionException ignore) {
            	// Ignore
            }
			return true;
		}

		@Override
		public boolean isReleasable() {
			return future.isDone();
		}
    }
    
	public static interface SerializableSupplier<V> extends Serializable, Supplier<V> {
    }
    
    public static <T> T getOn(ExecutorService executor, SerializableSupplier<T> supplier) {
    	Future<T> future = executor.submit(SerializableCallable.serializable(supplier::get));
    	for (;;) {
	    	try {
	    		// managedBlock will check this anyway, but it saves the extra object construction
	        	// when not running in a pool.
	        	if (ForkJoinTask.inForkJoinPool()) {
	        		ForkJoinPool.managedBlock(new FutureGetter<>(future));
	        	}
	    		return future.get();
			} catch (ExecutionException e) {
				// Accepting only Suppliers rather than Callables ensures that
				// only RuntimeExceptions are expected here.
				throw (RuntimeException)e.getCause();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
    	}
    }
    
    public static <T, R> Function<T, R> applyOn(ExecutorService executor, Function<T, R> fn) {
    	return x -> getOn(executor, () -> fn.apply(x));
    }

    public static <T> IntFunction<T> applyIntOn(ExecutorService executor, SerializableIntFunction<T> fn) {
    	return x -> getOn(executor, () -> fn.apply(x));
    }

    public static <T> Consumer<T> acceptOn(ExecutorService executor, Consumer<T> consumer) {
    	return x -> getOn(executor, () -> {consumer.accept(x); return null;});
    }
    
    public static IntConsumer acceptIntOn(ExecutorService executor, IntConsumer consumer) {
    	return x -> getOn(executor, () -> {consumer.accept(x); return null;});
    }
}
