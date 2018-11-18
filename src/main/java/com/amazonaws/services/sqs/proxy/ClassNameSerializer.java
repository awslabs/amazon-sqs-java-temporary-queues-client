package com.amazonaws.services.sqs.proxy;


public class ClassNameSerializer implements InvertibleFunction<Class<?>, String> {

    private final ClassLoader loader;
    
    public ClassNameSerializer() {
        this(ClassLoader.getSystemClassLoader());
    }
    
    public ClassNameSerializer(ClassLoader loader) {
        this.loader = loader;
    }
    
    @Override
    public String apply(Class<?> klass) {
        if (!klass.getClassLoader().equals(loader)) {
            throw new IllegalArgumentException();
        }
        return klass.getName();
    }

    @Override
    public Class<?> unapply(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            // TODO-RS: Better unchecked exception
            throw new RuntimeException(e);
        }
    }
}
