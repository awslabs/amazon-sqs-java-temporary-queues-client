package com.amazonaws.services.sqs.executors;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class SerializableReference<T> implements Serializable, Supplier<T>, AutoCloseable {

    private static final long serialVersionUID = -1228109108885696203L;

    protected static final ConcurrentMap<String, Object> globalScope = new ConcurrentHashMap<>();
    protected static final ThreadLocal<ConcurrentMap<String, Object>> currentScope = new ThreadLocal<ConcurrentMap<String, Object>>() {
        @Override
        protected ConcurrentMap<String, Object> initialValue() {
            return new ConcurrentHashMap<>();
        }
    };

    private final String name;
    private transient T object;
    private transient ConcurrentMap<String, Object> scope;

    public SerializableReference(String name, T object) {
        this(name, object, false);
    }

    public SerializableReference(String name, T object, boolean global) {
        this(name, object, global ? globalScope : new ConcurrentHashMap<>());
    }

    private SerializableReference(String name, T object, ConcurrentMap<String, Object> scope) {
        this.name = Objects.requireNonNull(name);
        this.object = Objects.requireNonNull(object);
        this.scope = Objects.requireNonNull(scope);

        Object existing = scope.putIfAbsent(name, object);
        if (existing != null) {
            throw new IllegalStateException("Serializable reference with ID \"" + name + "\" already in scope.");
        }
    }

    @Override
    public T get() {
        return object;
    }

    public <V> V withScope(Supplier<V> supplier) {
        ConcurrentMap<String, Object> oldScope = currentScope.get();
        currentScope.set(scope);
        try {
            return supplier.get();
        } finally {
            currentScope.set(oldScope);
        }
    }

    @Override
    public void close() {
        scope.remove(name);
    }
    
    @SuppressWarnings("unchecked")
    protected Object readResolve() {
        object = (T)currentScope.get().get(name);
        if (object != null) {
            scope = currentScope.get();
            return this;
        }

        object = (T)globalScope.get(name); 
        if (object != null) {
            scope = globalScope;
            return this;
        }

        throw new IllegalStateException("Could not locate object with ID \"" + name + "\"");
    }

    private class Proxy implements Serializable {
        
        private static final long serialVersionUID = -6050603080658976720L;

        protected Object readResolve() {
            return object;
        }
    }
    
    /**
     * Returns a Serializable object that resolves to the referenced object when deserialized.
     */
    public Serializable proxy() {
        return new Proxy();
    }
}
