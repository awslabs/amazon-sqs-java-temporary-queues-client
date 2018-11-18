package com.amazonaws.services.sqs.proxy;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

public class Invocation implements Callable<Object> {

	private final Object instance;
	private final Method method;
	private final Object[] arguments;
	
	public Invocation(Object instance, Method method, Object[] arguments) {
		super();
		this.instance = instance;
		this.method = method;
		this.arguments = arguments;
	}
	
	public Object getInstance() {
		return instance;
	}
	
	public Method getMethod() {
		return method;
	}
	
	public Object[] getArguments() {
		return arguments;
	}

    @Override
    public Object call() throws Exception {
        return method.invoke(instance, arguments);
    }
	
	
}
