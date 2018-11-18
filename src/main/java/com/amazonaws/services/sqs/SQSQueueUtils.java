package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.ExecutorUtils.acceptIntOn;
import static com.amazonaws.services.sqs.ExecutorUtils.acceptOn;
import static com.amazonaws.services.sqs.ExecutorUtils.applyIntOn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSQueueUtils {

	public static final String RESPONSE_QUEUE_URL_ATTRIBUTE_NAME = "ResponseQueueUrl";
	
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html
    private static final String VALID_QUEUE_NAME_CHARACTERS;
    static {
    	StringBuilder builder = new StringBuilder();
    	IntStream.rangeClosed('a', 'z').forEach(i -> builder.append((char)i));
    	IntStream.rangeClosed('A', 'Z').forEach(i -> builder.append((char)i));
    	IntStream.rangeClosed('0', '9').forEach(i -> builder.append((char)i));
    	builder.append('-');
    	builder.append('_');
    	VALID_QUEUE_NAME_CHARACTERS = builder.toString();
    }
    
    public static boolean isQueueEmpty(AmazonSQS sqs, String queueUrl) {
		QueueAttributeName[] messageCountAttrs = {
                QueueAttributeName.ApproximateNumberOfMessages,
                QueueAttributeName.ApproximateNumberOfMessagesDelayed,
                QueueAttributeName.ApproximateNumberOfMessagesNotVisible
        };
        
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .withAttributeNames(messageCountAttrs);
        GetQueueAttributesResult result = sqs.getQueueAttributes(getQueueAttributesRequest);
        Map<String, String> attrValues = result.getAttributes();
        return Stream.of(messageCountAttrs).allMatch(attr ->
                Long.parseLong(attrValues.get(attr.name())) == 0);
	}
	
    public static boolean pollingWait(long timeout, TimeUnit unit, Supplier<Boolean> test) throws InterruptedException {
    	long remainingSeconds = TimeUnit.SECONDS.convert(timeout, unit);
		while (!test.get()) {
            if (remainingSeconds <= 0) {
                return false;
            }
            Thread.sleep(1000);
            remainingSeconds--;
        }
		return true;
    }
    
	public static boolean awaitEmptyQueue(AmazonSQS sqs, String queueUrl, long timeout, TimeUnit unit) throws InterruptedException {
		// There's no way to be directly notified unfortunately.
		return pollingWait(timeout, unit, () -> isQueueEmpty(sqs, queueUrl));
	}
    
    public static void forEachQueue(ExecutorService executor, AmazonSQS sqs, String prefix, int limit, Consumer<String> action) {
	    List<String> queueUrls = sqs.listQueues(prefix).getQueueUrls();
	    if (queueUrls.size() >= limit) {
	    	// Manually work around the 1000 queue limit by forking for each
	    	// possible next character. Yes this is exponential with a factor of
	    	// 64, but we only fork when the results are more than 1000.
	        VALID_QUEUE_NAME_CHARACTERS
	        		.chars()
				    .parallel()
				    .forEach(acceptIntOn(executor, c ->
				    	forEachQueue(executor, sqs, prefix + (char)c, limit, action)));
	    } else {
	    	queueUrls.forEach(acceptOn(executor, action));
	    }
    }
    
    public static Stream<String> listQueuesStream(ExecutorService executor, Function<String, List<String>> lister, String prefix, int limit) {
	    List<String> queueUrls = lister.apply(prefix);
	    if (queueUrls.size() >= limit) {
	    	// Manually work around the 1000 queue limit by forking for each
	    	// possible next character. Yes this is exponential with a factor of
	    	// 64, but we only fork when the results are more than 1000.
	    	return VALID_QUEUE_NAME_CHARACTERS
	    				.chars()
	    				.parallel()
					    .mapToObj(applyIntOn(executor, c ->
					    	listQueuesStream(executor, lister, prefix + (char)c, limit)))
				        .flatMap(Function.identity());
	    } else {
	    	return queueUrls.stream();
	    }
    }
    
    /**
     * Sends a message and returns the URL of a newly-created queue for its response.
     * <p>
     * The response queue should be deleted once the expected response is received
     * or the need for it has expired.
     */
    public static String sendMessageWithResponseQueue(AmazonSQS sqs, SendMessageRequest request) {
    	String responseQueue = sqs.createQueue(UUID.randomUUID().toString()).getQueueUrl();

    	SendMessageRequest requestWithResponseUrl = copyWithExtraAttributes(request,
    			Collections.singletonMap(RESPONSE_QUEUE_URL_ATTRIBUTE_NAME, 
    					new MessageAttributeValue().withDataType("String").withStringValue(responseQueue)));
    	sqs.sendMessage(requestWithResponseUrl);

    	return responseQueue;
    }
    
    // TODO-RS: Add sendRequestAndGetResponses for multiple response messages?
    public static Message sendMessageAndGetResponse(AmazonSQS sqs, SendMessageRequest request, int waitTimeSeconds) throws TimeoutException {
    	return receiveResponse(sqs, sendMessageWithResponseQueue(sqs, request), waitTimeSeconds);
	}
	
    public static CompletableFuture<Message> sendMessageAndGetResponseAsync(AmazonSQS sqs, SendMessageRequest request) {
    	String responseQueueUrl = sendMessageWithResponseQueue(sqs, request);
    	
    	CompletableFuture<Message> future = new CompletableFuture<>();
    	// TODO-RS: accept an AmazonSQSAsync instead and use its threads instead of our own.
    	SQSMessageConsumer consumer = new SQSMessageConsumer(sqs, responseQueueUrl, future::complete);
    	consumer.start();
    	future.whenComplete((message, exception) -> {
    		consumer.shutdown();
    		sqs.deleteQueue(responseQueueUrl);
    	});
    	return future;
    }
    
	public static String responseQueueUrl(Message message) {
    	MessageAttributeValue attribute = message.getMessageAttributes().get(RESPONSE_QUEUE_URL_ATTRIBUTE_NAME);
    	return attribute != null ? attribute.getStringValue() : null;
    }
    
    public static void sendResponse(AmazonSQS sqs, Message message, SendMessageRequest request) {
    	String responseQueueUrl = responseQueueUrl(message);
    	if (responseQueueUrl != null) {
    		request.setQueueUrl(responseQueueUrl);
    		sendResponse(sqs, request);
    	} else {
    		// TODO-RS: CW metric and log
    		System.out.println("warning: tried to send response when none was requested");
    	}
    }
    
	public static void sendResponse(AmazonSQS sqs, SendMessageRequest request) {
		try {
			sqs.sendMessage(request);
		} catch (QueueDoesNotExistException e) {
			// Stale request, ignore
			System.out.println("Ignoring request with deleted reply queue: " + request.getQueueUrl());
		}
    }
    
	public static Message receiveResponse(AmazonSQS sqs, String responseQueueUrl, int waitTimeSeconds) throws TimeoutException {
		try {
			ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
					.withQueueUrl(responseQueueUrl)
					.withMaxNumberOfMessages(1)
					.withWaitTimeSeconds(waitTimeSeconds);
			List<Message> messages = sqs.receiveMessage(receiveRequest).getMessages();
			if (messages.isEmpty()) {
				throw new TimeoutException();
			} else {
				return messages.get(0);
			}
		} finally {
			sqs.deleteQueue(responseQueueUrl);
		}
	}
	
	// TODO-RS: The upcoming V2 of the AWS SDK for Java will include versions of API operations
	// that use CompletableFutures directly. 
	public static <REQUEST extends AmazonWebServiceRequest, RESULT> Function<REQUEST, CompletableFuture<RESULT>> completable(
				BiFunction<REQUEST, AsyncHandler<REQUEST, RESULT>, Future<RESULT>> operation) {

		return request -> {
			CompletableFuture<RESULT> future = new CompletableFuture<>();
			
			operation.apply(request, new AsyncHandler<REQUEST, RESULT>() {
				@Override
				public void onSuccess(REQUEST request, RESULT result) {
					future.complete(result);
				}
				
				@Override
				public void onError(Exception exception) {
					future.completeExceptionally(exception);
				}
			});
			
			return future;
		};
	}
	
    public static CreateQueueRequest copyWithExtraAttributes(CreateQueueRequest request, Map<String, String> extraAttrs) {
        Map<String, String> newAttributes = new HashMap<>(request.getAttributes());
        newAttributes.putAll(extraAttrs);
        
        // Clone to create a shallow copy that includes the superclass properties.
        return request.clone()
                      .withQueueName(request.getQueueName())
                      .withAttributes(newAttributes);
    }
    
    public static SendMessageRequest copyWithExtraAttributes(SendMessageRequest request, Map<String, MessageAttributeValue> extraAttrs) {
        Map<String, MessageAttributeValue> newAttributes = new HashMap<>(request.getMessageAttributes());
        newAttributes.putAll(extraAttrs);
        
        // Clone to create a shallow copy that includes the superclass properties.
        return request.clone()
                      .withQueueUrl(request.getQueueUrl())
                      .withMessageBody(request.getMessageBody())
                      .withMessageAttributes(newAttributes)
                      .withDelaySeconds(request.getDelaySeconds());
    }
    
    public static SendMessageRequest sendMessageRequest(Message message) {
        return new SendMessageRequest()
                .withMessageBody(message.getBody())
                .withMessageAttributes(message.getMessageAttributes());
    }
}
