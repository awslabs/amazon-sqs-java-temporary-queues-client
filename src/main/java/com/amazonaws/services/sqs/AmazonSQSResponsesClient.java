package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;
import com.amazonaws.services.sqs.responsesapi.MessageContent;

public class AmazonSQSResponsesClient implements AmazonSQSWithResponses {
    
    public static final String RESPONSE_QUEUE_URL_ATTRIBUTE_NAME = "ResponseQueueUrl";
    private static final String HOST_QUEUE_NAME_PREFIX = "__HostQueue";
    
    private final ConcurrentMap<Map<String, String>, String> hostQueueUrls = new ConcurrentHashMap<>();
    
    private final AmazonSQS sqs;
    private final String prefix;
    
    public AmazonSQSResponsesClient(AmazonSQS sqs) {
        // TODO-RS: Be smarter about this: include host name, etc.
        // TODO-RS: Include SecureRandom so this is not guessable without ListQueues
        this(sqs, UUID.randomUUID().toString());
    }

    public AmazonSQSResponsesClient(AmazonSQS sqs, String clientId) {
        this.sqs = makeWrappedSQSClient(sqs);
        this.prefix = HOST_QUEUE_NAME_PREFIX + "_" + clientId + "_";
    }
    
    private static AmazonSQS makeWrappedSQSClient(AmazonSQS sqs) {
        // TODO-RS: Determine the right strategy for naming the sweeping queue.
        // It needs to be shared between different clients, but testing friendly!
        // TODO-RS: Configure a tight MessageRetentionPeriod
        String sweepingQueueUrl = sqs.createQueue(HOST_QUEUE_NAME_PREFIX + "_Sweeping").getQueueUrl();
        AmazonSQS deleter = new AmazonSQSIdleQueueDeletingClient(sqs, HOST_QUEUE_NAME_PREFIX, sweepingQueueUrl);
        return new AmazonSQSVirtualQueuesClient(deleter);
    }
    
    public CreateQueueResult createQueue(CreateQueueRequest request) {
        String hostQueueUrl = hostQueueUrls.computeIfAbsent(request.getAttributes(), attributes -> {
            String name = prefix + hostQueueUrls.size();
            return sqs.createQueue(request.withQueueName(name)).getQueueUrl();
        });
        CreateQueueRequest createVirtualQueueRequest = SQSQueueUtils.copyWithExtraAttributes(request, 
                Collections.singletonMap(AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE,
                                         hostQueueUrl));
        return sqs.createQueue(createVirtualQueueRequest);
    }
    
    @Override
    public void shutdown() {
        try {
        	hostQueueUrls.values().forEach(sqs::deleteQueue);
        } finally {
            sqs.shutdown();
        }
    }

	@Override
	public Message sendMessageAndGetResponse(SendMessageRequest request, int timeout, TimeUnit unit) throws TimeoutException {
	    return receiveResponse(sendMessageWithResponseQueue(request), (int)unit.toSeconds(timeout));
	}

	@Override
	public CompletableFuture<Message> sendMessageAndGetResponseAsync(SendMessageRequest request) {
	    String responseQueueUrl = sendMessageWithResponseQueue(request);
        
        CompletableFuture<Message> future = new CompletableFuture<>();
        // TODO-RS: accept an AmazonSQSAsync instead and use its threads instead of our own.
        // TODO-RS: complete the future exceptionally, for the right set of exceptions
        SQSMessageConsumer consumer = new SQSMessageConsumer(sqs, responseQueueUrl, future::complete);
        consumer.start();
        future.whenComplete((message, exception) -> {
            consumer.shutdown();
            sqs.deleteQueue(responseQueueUrl);
        });
        return future;
	}

	@Override
	public void sendResponseMessage(MessageContent request, MessageContent response) {
	    MessageAttributeValue attribute = request.getMessageAttributes().get(RESPONSE_QUEUE_URL_ATTRIBUTE_NAME);
        
	    if (attribute != null) {
	        String replyQueueUrl = attribute.getStringValue();
	        try {
	            SendMessageRequest responseRequest = new SendMessageRequest()
	                    .withMessageBody(response.getMessageBody())
	                    .withMessageAttributes(response.getMessageAttributes())
	                    .withQueueUrl(replyQueueUrl);
                sqs.sendMessage(responseRequest);
            } catch (QueueDoesNotExistException e) {
                // Stale request, ignore
                System.out.println("Ignoring request with deleted reply queue: " + replyQueueUrl);
            }
	    } else {
	        // TODO-RS: CW metric and log
            System.out.println("warning: tried to send response when none was requested");
	    }
	}
	
	private String sendMessageWithResponseQueue(SendMessageRequest request) {
        String responseQueue = createQueue(new CreateQueueRequest().withQueueName(UUID.randomUUID().toString())).getQueueUrl();

        SendMessageRequest requestWithResponseUrl = SQSQueueUtils.copyWithExtraAttributes(request,
                Collections.singletonMap(RESPONSE_QUEUE_URL_ATTRIBUTE_NAME, 
                        new MessageAttributeValue().withDataType("String").withStringValue(responseQueue)));
        sqs.sendMessage(requestWithResponseUrl);

        return responseQueue;
    }
	
	@Override
	public boolean isResponseMessageRequested(MessageContent requestMessage) {
	    return requestMessage.getMessageAttributes().containsKey(RESPONSE_QUEUE_URL_ATTRIBUTE_NAME);
	}
	
	public Message receiveResponse(String responseQueueUrl, int waitTimeSeconds) throws TimeoutException {
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
}
