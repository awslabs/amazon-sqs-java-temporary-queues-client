package com.amazonaws.services.sqs.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.SQSQueueUtils;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;

public class SQSInvocationHandler implements InvocationHandler {

	private final AmazonSQSWithResponses sqs;
	private final String queueUrl;
	private final SQSProxy sqsProxy;
    
	public SQSInvocationHandler(AmazonSQSWithResponses sqs, String queueUrl, PolymorphicInvertibleFunction<Object, String> serializer) {
		this.sqs = sqs;
		this.queueUrl = queueUrl;
		this.sqsProxy = new SQSProxy(sqs, serializer);
	}
	
	// TODO-RS: Async versions
	@Override
	public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
	    Invocation invocation = new Invocation(proxy, method, arguments);
	    Message message = sqsProxy.toMessage(invocation);
	    
		SendMessageRequest request = SQSQueueUtils.sendMessageRequest(message).withQueueUrl(queueUrl);
		
		// TODO-RS: Configure timeout
		Message response = sqs.sendMessageAndGetResponse(request, 5, TimeUnit.SECONDS);
		
		return sqsProxy.getResult(invocation, response.getBody());
	}
}
