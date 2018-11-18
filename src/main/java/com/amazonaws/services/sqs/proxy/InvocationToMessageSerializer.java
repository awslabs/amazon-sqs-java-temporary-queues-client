package com.amazonaws.services.sqs.proxy;

import java.lang.reflect.Method;
import java.util.function.Function;
import java.util.stream.IntStream;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

// TODO-RS: Message is used only for the body and message attributes properties, but it really represents
// a received message and so has other irrelevant fields such as the receipt handle. This library could
// probably benefit from a separate, simpler message datatype with clear relationships to
// SendMessageRequest, SendMessageBatchRequestEntry and Message. It could even be a more generic
// data type for any data type with String-based body and attributes.
public class InvocationToMessageSerializer implements InvertibleFunction<Invocation, Message> {

    public static final String TARGET_ATTRIBUTE_NAME = "Target";
    public static final String ARGUMENT_ATTRIBUTE_NAME_PREFIX = "Argument";
    
    private final PolymorphicInvertibleFunction<Object, String> serializer;
	private final MethodSerializer methodSerializer = new MethodSerializer(new ClassNameSerializer());
    
    public InvocationToMessageSerializer(PolymorphicInvertibleFunction<Object, String> serializer) {
        this.serializer = serializer;
    }
    
	@Override
	public Message apply(Invocation t) {
	    Message result = new Message().withBody(methodSerializer.apply(t.getMethod()));
	    serializeToAttribute(result, t.getMethod().getDeclaringClass(), TARGET_ATTRIBUTE_NAME, t.getInstance());
	    Class<?>[] parameterTypes = t.getMethod().getParameterTypes();
        IntStream.range(0, parameterTypes.length)
	             .forEach(index -> serializeToAttribute(result,
	                                                    parameterTypes[index],
	                                                    ARGUMENT_ATTRIBUTE_NAME_PREFIX + index,
	                                                    t.getArguments()[index]));
	    return result;
	}

	private void serializeToAttribute(Message message, Class<?> type, String attributeName, Object value) {
	    message.addMessageAttributesEntry(attributeName, 
	            new MessageAttributeValue().withDataType("String")
	                                       .withStringValue(serializer.apply(type, value)));
	}
	
	@Override
	public Invocation unapply(Message message) {
        Method method = methodSerializer.unapply(message.getBody());
        Object target = deserializeAttribute(message, method.getDeclaringClass(), TARGET_ATTRIBUTE_NAME);
        Object[] arguments = IntStream.range(0, method.getParameterTypes().length)
                                      .mapToObj(index -> deserializeArgument(message, method, index))
                                      .toArray();
        return new Invocation(target, method, arguments);
	}
	
	private Object deserializeArgument(Message message, Method method, int index) {
	    return deserializeAttribute(message, method.getParameterTypes()[index], ARGUMENT_ATTRIBUTE_NAME_PREFIX + index);
	}
	
	private Object deserializeAttribute(Message message, Class<?> type, String attributeName) {
	    String serialized = message.getMessageAttributes().get(attributeName).getStringValue();
        return serializer.forClass(type).unapply(serialized);
	}
}
