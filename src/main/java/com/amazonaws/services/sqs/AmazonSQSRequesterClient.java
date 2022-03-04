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

import com.amazonaws.services.sqs.util.Constants;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Implementation of the request/response interfaces that creates a single
 * temporary queue for each response message.
 */
class AmazonSQSRequesterClient implements AmazonSQSRequester {
    private final SqsClient sqs;
    private final String queuePrefix;
    private final Map<String, String> queueAttributes;
    private final Consumer<Exception> exceptionHandler;
    
    private final Set<SQSMessageConsumer> responseConsumers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private Runnable shutdownHook;
    
    AmazonSQSRequesterClient(SqsClient sqs, String queuePrefix, Map<String, String> queueAttributes) {
        this(sqs, queuePrefix, queueAttributes, SQSQueueUtils.DEFAULT_EXCEPTION_HANDLER);
    }

    AmazonSQSRequesterClient(SqsClient sqs, String queuePrefix, Map<String, String> queueAttributes,
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
    public SqsClient getAmazonSQS() {
        return sqs;
    }

    @Override
    public Message sendMessageAndGetResponse(SendMessageRequest request, int timeout, TimeUnit unit) {
        return SQSQueueUtils.waitForFuture(sendMessageAndGetResponseAsync(request, timeout, unit));
    }

    @Override
    public CompletableFuture<Message> sendMessageAndGetResponseAsync(SendMessageRequest request, int timeout, TimeUnit unit) {
        String queueName = queuePrefix + UUID.randomUUID().toString();
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributesWithStrings(queueAttributes).build();
        String responseQueueUrl = sqs.createQueue(createQueueRequest).queueUrl();

        SendMessageRequest requestWithResponseUrl = SQSQueueUtils.copyWithExtraAttributes(request,
                Collections.singletonMap(Constants.RESPONSE_QUEUE_URL_ATTRIBUTE_NAME,
                        MessageAttributeValue.builder().dataType("String").stringValue(responseQueueUrl).build()));
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
            // TODO-RS: Rethink this to avoid subclassing a class that now has a proper builder.
            super(AmazonSQSRequesterClient.this.sqs, queueUrl, message -> {}, null, AmazonSQSRequesterClient.this.exceptionHandler);
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
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
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
