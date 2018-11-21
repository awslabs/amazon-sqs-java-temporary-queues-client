package com.amazonaws.services.sqs.proxy;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.function.Consumer;

import com.amazonaws.services.sqs.SQSQueueUtils;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;
import com.amazonaws.services.sqs.responsesapi.MessageContent;

public class SQSProxy implements Consumer<Message> {

	private final AmazonSQSWithResponses sqs;
	private final PolymorphicInvertibleFunction<Object, String> serializer;
	private final InvocationToMessageSerializer invocationSerializer;
	private final InvertibleFunction<Throwable, String> exceptionSerializer;
	
	public SQSProxy(AmazonSQSWithResponses sqs, PolymorphicInvertibleFunction<Object, String> serializer) {
		this.sqs = sqs;
		this.serializer = serializer;
		this.invocationSerializer = new InvocationToMessageSerializer(serializer);
		this.exceptionSerializer = serializer.forClass(Throwable.class);
	}
	
	public Message toMessage(Invocation invocation) {
	    return invocationSerializer.apply(invocation);
	}
	
	private CompletedFutureToMessageSerializer<Object> getFutureSerializer(Class<?> type) {
	    InvertibleFunction<Object, String> returnValueSerializer = serializer.restricted(type);
        return new CompletedFutureToMessageSerializer<>(returnValueSerializer, exceptionSerializer);
	}
	
	public void accept(Message message) {
	    Invocation invocation = invocationSerializer.unapply(message);
	    
	    FutureTask<Object> task = new FutureTask<>(invocation);
	    task.run();
		
        String response = getFutureSerializer(invocation.getMethod().getReturnType()).apply(task);
		
		sqs.sendResponseMessage(MessageContent.fromMessage(message),
		                        new MessageContent(response));
	}
	
	public Object getResult(Invocation invocation, String response) throws Throwable {
	    try {
            return getFutureSerializer(invocation.getMethod().getReturnType()).unapply(response).get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
	}
	
}
