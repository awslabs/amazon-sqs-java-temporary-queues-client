package com.amazonaws.services.sqs;

import java.util.ArrayList;
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
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;

public class AmazonSQSTemporaryQueuesClient extends AbstractAmazonSQSClientWrapper implements AmazonSQSWithResponses {
    
    private static final String HOST_QUEUE_NAME_PREFIX = "__HostQueue";
    
    private final ConcurrentMap<Map<String, String>, String> hostQueueUrls = new ConcurrentHashMap<>();
    
    private final String prefix;
    
    public AmazonSQSTemporaryQueuesClient(AmazonSQS sqs) {
        // TODO-RS: Be smarter about this: include host name, etc.
        // TODO-RS: Include SecureRandom so this is not guessable without ListQueues
        this(sqs, UUID.randomUUID().toString());
    }

    public AmazonSQSTemporaryQueuesClient(AmazonSQS sqs, String clientId) {
        super(makeWrappedSQSClient(sqs));
        this.prefix = HOST_QUEUE_NAME_PREFIX + "_" + clientId + "_";
    }
    
    private static AmazonSQS makeWrappedSQSClient(AmazonSQS sqs) {
        // TODO-RS: Determine the right strategy for naming the sweeping queue.
        // It needs to be shared between different clients, but testing friendly!
        String sweepingQueueUrl = sqs.createQueue(HOST_QUEUE_NAME_PREFIX + "_Sweeping").getQueueUrl();
        AmazonSQS deleter = new AmazonSQSIdleQueueDeletingClient(sqs, HOST_QUEUE_NAME_PREFIX, sweepingQueueUrl);
        return new AmazonSQSVirtualQueuesClient(deleter);
    }
    
    @Override
    public CreateQueueResult createQueue(CreateQueueRequest request) {
        String hostQueueUrl = hostQueueUrls.computeIfAbsent(request.getAttributes(), attributes -> {
            String name = prefix + hostQueueUrls.size();
            return amazonSqsToBeExtended.createQueue(request.withQueueName(name)).getQueueUrl();
        });
        CreateQueueRequest createVirtualQueueRequest = SQSQueueUtils.copyWithExtraAttributes(request, 
                Collections.singletonMap(AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE,
                                         hostQueueUrl));
        return super.createQueue(createVirtualQueueRequest);
    }
    
    @Override
    public void shutdown() {
        try {
        	hostQueueUrls.values().forEach(amazonSqsToBeExtended::deleteQueue);
        } finally {
            amazonSqsToBeExtended.shutdown();
        }
    }

	@Override
	public Message sendMessageAndGetResponse(SendMessageRequest request, int timeout, TimeUnit unit) throws TimeoutException {
		return SQSQueueUtils.sendMessageAndGetResponse(this, request, (int)unit.toSeconds(timeout));
	}

	@Override
	public CompletableFuture<Message> sendMessageAndGetResponseAsync(SendMessageRequest request) {
		return SQSQueueUtils.sendMessageAndGetResponseAsync(this, request);
	}

	@Override
	public ReceiveMessageResult receiveMessage(ReceiveMessageRequest request) {
		List<String> messageAttributeNames = new ArrayList<>(request.getMessageAttributeNames());
		messageAttributeNames.add(SQSQueueUtils.RESPONSE_QUEUE_URL_ATTRIBUTE_NAME);
		// TODO-RS: Don't modify requests!
		return amazonSqsToBeExtended.receiveMessage(request.withMessageAttributeNames(messageAttributeNames));
	}

	@Override
	public void sendResponseMessage(Message message, SendMessageRequest request) {
		SQSQueueUtils.sendResponse(this, message, request);
	}
}
