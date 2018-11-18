package com.amazonaws.services.sqs.proxy;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class MethodSerializer implements InvertibleFunction<Method, String> {

    private final ClassNameSerializer classSerializer;
    
    public MethodSerializer(ClassNameSerializer classSerializer) {
        this.classSerializer = classSerializer;
    }
    
    @Override
    public String apply(Method method) {
        String result = classSerializer.apply(method.getDeclaringClass()) + "#" + method.getName() + "(";
        Stream<CharSequence> paramTypeNames = Arrays.stream(method.getParameterTypes()).map(classSerializer::apply);
        result += String.join(", ", paramTypeNames::iterator);
        return result + ")";
    }

    @Override
    public Method unapply(String signature) {
        Pattern regexp = Pattern.compile("/^(.*)#(.*)\\((.*)\\)/$");
        Matcher matcher = regexp.matcher(signature);
        if (matcher.find()) {
            String declaringClassName = matcher.group(1);
            String methodName = matcher.group(2);
            String parameters = matcher.group(3);
            
            Class<?> declaringClass = classSerializer.unapply(declaringClassName);
            Class<?>[] parameterTypes = new JoiningSerializer(", ").unapply(parameters)
                                                                   .stream()
                                                                   .map(classSerializer::unapply)
                                                                   .toArray(Class<?>[]::new);
            try {
                return declaringClass.getDeclaredMethod(methodName, parameterTypes);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

}
