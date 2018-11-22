package com.amazonaws.services.sqs.executors;

import java.io.ObjectStreamException;
import java.io.Serializable;
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
	private final transient T object;
	private final ConcurrentMap<String, Object> scope;
	
	public SerializableReference(String name, T object) {
		this(name, object, globalScope);
	}
	
	public SerializableReference(String name, T object, ConcurrentMap<String, Object> scope) {
        this.name = name;
        this.object = object;
        this.scope = scope;
        
        Object existing = scope.putIfAbsent(name, object);
        if (existing != null) {
            throw new IllegalStateException();
        }
    }
    
    @Override
	public T get() {
		return object;
	}
	
	public <V> V withScope(ConcurrentMap<String, Object> scope, Supplier<V> supplier) {
	    ConcurrentMap<String, Object> oldScope = currentScope.get();
	    currentScope.set(scope);
	    try {
	        return supplier.get();
	    } finally {
	        currentScope.set(oldScope);
	    }
	}
	
	private Object readResolve() throws ObjectStreamException {
	    Object result = currentScope.get().get(name);
	    if (result != null) {
	        return result;
	    }
	    
		result = globalScope.get(name); 
		if (result != null) {
		    return result;
		}

		throw new IllegalStateException("Could not locate object with ID " + name);
	}
	
	@Override
	public void close() {
	    scope.remove(name);
	}
}
