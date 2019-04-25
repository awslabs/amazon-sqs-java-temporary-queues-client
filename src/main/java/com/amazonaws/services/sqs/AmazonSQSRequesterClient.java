package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSQueueUtils;

/**
 * Implementation of the request/response interfaces that creates a single
 * temporary queue for each response message.
 */
class AmazonSQSRequesterClient implements AmazonSQSRequester {

    public static final String RESPONSE_QUEUE_URL_ATTRIBUTE_NAME = "ResponseQueueUrl";

    private final AmazonSQS sqs;
    private final String queuePrefix;
    private final Map<String, String> queueAttributes;
    private final Consumer<Exception> exceptionHandler;
    
    private final Set<SQSMessageConsumer> responseConsumers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private Runnable shutdownHook;
    
    AmazonSQSRequesterClient(AmazonSQS sqs, String queuePrefix, Map<String, String> queueAttributes) {
        this(sqs, queuePrefix, queueAttributes, SQSQueueUtils.DEFAULT_EXCEPTION_HANDLER);
    }

    AmazonSQSRequesterClient(AmazonSQS sqs, String queuePrefix, Map<String, String> queueAttributes,
                Consumer<Exception> exceptionHandler) {
        this.sqs = sqs;
        this.queuePrefix = queuePrefix;
        this.queueAttributes = new HashMap<>(queueAttributes);
        this.exceptionHandler = exceptionHandler;
    }

    public void setShutdownHook(Runnable shutdownHook) {
        this.shutdownHook = shutdownHook;
    }

    @Override
    public AmazonSQS getAmazonSQS() {
        return sqs;
    }

    @Override
    public Message sendMessageAndGetResponse(SendMessageRequest request, int timeout, TimeUnit unit) {
        return SQSQueueUtils.waitForFuture(sendMessageAndGetResponseAsync(request, timeout, unit));
    }

    @Override
    public CompletableFuture<Message> sendMessageAndGetResponseAsync(SendMessageRequest request, int timeout, TimeUnit unit) {
        String queueName = queuePrefix + UUID.randomUUID().toString();
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueName)
                .withAttributes(queueAttributes);
        String responseQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

        SendMessageRequest requestWithResponseUrl = SQSQueueUtils.copyWithExtraAttributes(request,
                Collections.singletonMap(RESPONSE_QUEUE_URL_ATTRIBUTE_NAME, 
                        new MessageAttributeValue().withDataType("String").withStringValue(responseQueueUrl)));
        // TODO-RS: Should be using sendMessageAsync
        sqs.sendMessage(requestWithResponseUrl);

        CompletableFuture<Message> future = new CompletableFuture<>();
        
        // TODO-RS: accept an AmazonSQSAsync instead and use its threads instead of our own.
        // TODO-RS: complete the future exceptionally, for the right set of SQS exceptions
        SQSMessageConsumer consumer = new ResponseListener(responseQueueUrl, future);
        responseConsumers.add(consumer);
        consumer.runFor(timeout, unit);
        return future;
    }

    private class ResponseListener extends SQSMessageConsumer {

        private final CompletableFuture<Message> future;
        
        public ResponseListener(String queueUrl, CompletableFuture<Message> future) {
            super(AmazonSQSRequesterClient.this.sqs, queueUrl, null, null, AmazonSQSRequesterClient.this.exceptionHandler);
            this.future = future;
        }
        
        @Override
        protected void accept(Message message) {
            future.complete(message);
            super.shutdown();
        }
        
        @Override
        protected void runShutdownHook() {
            future.completeExceptionally(new TimeoutException());
            sqs.deleteQueue(queueUrl);
            responseConsumers.remove(this);
        }
    }
    
    @Override
    public void shutdown() {
        responseConsumers.forEach(SQSMessageConsumer::terminate);
        if (shutdownHook != null) {
            shutdownHook.run();
        }
    }
}
