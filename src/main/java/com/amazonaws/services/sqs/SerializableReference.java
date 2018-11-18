package com.amazonaws.services.sqs;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class SerializableReference<T> implements Serializable, Supplier<T> {

	private static final long serialVersionUID = -1228109108885696203L;

	protected static final Map<String, Object> references = new ConcurrentHashMap<>();
	
	private final String name;
	private final transient T object;
	
	public SerializableReference(String name, T object) {
		this.name = name;
		this.object = object;
		Object existing = references.putIfAbsent(name, this);
		if (existing != null) {
			throw new IllegalStateException();
		}
	}
	
	@Override
	public T get() {
		return object;
	}
	
	private Object readResolve() throws ObjectStreamException {
		Object result = references.get(name); 
		if (result == null) {
			throw new IllegalStateException("Could not locate object with ID " + name);
		}
		return result;
	}
}
