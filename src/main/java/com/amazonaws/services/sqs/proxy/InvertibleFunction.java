package com.amazonaws.services.sqs.proxy;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Represents a reversible function that accepts one argument and produces a result.
 * <p>
 * There are no hard guarantees around equivalence between {@link #apply} and {@link #unapply},
 * but it is generally preferable for 
 * 
 * <pre>
 * t.equals(function.unapply(function.apply(t))
 * </pre>
 * 
 * to be true for all possible values of {@code t}. However, invertible functions are particularly
 * useful for modeling serialization functions, where {@link #apply} and {@link #unapply}
 * may not be invoked in the same context or even on the same JVM.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 */
public interface InvertibleFunction<T, R> {

    /**
     * Applies this function to the given argument.
     */
	R apply(T t);
	
	/**
     * Applies the inverse of this function to the given argument.
     */
	T unapply(R r);
	
	static <T, R> InvertibleFunction<T, R> of(Function<T, R> function, Function<? super R, ? extends T> inverse) {
		return new InvertibleFunction<T, R>() {
			@Override
			public R apply(T t) {
				return function.apply(t);
			}
			@Override
			public T unapply(R r) {
				return inverse.apply(r);
			}
		};
	}
	
	default InvertibleFunction<R, T> inverse() {
		return of(this::unapply, this::apply);
	}
	
	/**
     * Returns a composed invertible function that first applies the {@code before}
     * function to its input, and then applies this function to the result.
     * If evaluation of either function throws an exception, it is relayed to
     * the caller of the composed function.
     *
     * @param <V> the type of input to the {@code before} function, and to the
     *           composed function
     * @param before the function to apply before this function is applied
     * @return a composed function that first applies the {@code before}
     * function and then applies this function
     * @throws NullPointerException if before is null
     *
     * @see #andThen(Function)
     */
    default <V> InvertibleFunction<V, R> compose(InvertibleFunction<V, T> before) {
        Objects.requireNonNull(before);
        return of(v -> apply(before.apply(v)),
      		      t -> before.unapply(unapply(t)));
    }

    /**
     * Returns a composed invertible function that first applies this function to
     * its input, and then applies the {@code after} function to the result.
     * If evaluation of either function throws an exception, it is relayed to
     * the caller of the composed function.
     *
     * @param <V> the type of output of the {@code after} function, and of the
     *           composed function
     * @param after the function to apply after this function is applied
     * @return a composed function that first applies this function and then
     * applies the {@code after} function
     * @throws NullPointerException if after is null
     *
     * @see #compose(Function)
     */
    default <V> InvertibleFunction<T, V> andThen(InvertibleFunction<R, V> after) {
        Objects.requireNonNull(after);
        return of(t -> after.apply(apply(t)),
        		  v -> unapply(after.unapply(v)));
    }

    /**
     * Returns an invertible function that always returns its input argument.
     */
    static <T> InvertibleFunction<T, T> identity() {
        return of(Function.identity(), Function.identity());
    }
    
    static <T, R extends T> InvertibleFunction<T, R> cast(Class<R> klass) {
        return of(klass::cast, r -> r);
    }
    
    @SuppressWarnings("unchecked")
    static <T, R> InvertibleFunction<T, R> uncheckedCast() {
        return of(t -> (R)t, r -> (T)r);
    }
}
