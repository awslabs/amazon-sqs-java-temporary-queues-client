package com.amazonaws.services.sqs.util;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.MessageContent;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.TagQueueRequest;
import software.amazon.awssdk.services.sqs.model.TagQueueResponse;
import software.amazon.awssdk.services.sqs.model.UntagQueueRequest;
import software.amazon.awssdk.services.sqs.model.UntagQueueResponse;

public class MockSQSQueue {

    private final String queueName;
    private final BlockingQueue<MessageContent> visibleMessages = new ArrayBlockingQueue<>(10);
    private final Map<String, MessageContent> inflight = new HashMap<>();
    private final ConcurrentMap<String, String> tags = new ConcurrentHashMap<>();
    
    public MockSQSQueue(String queueName) {
        this.queueName = queueName;
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public SendMessageResponse sendMessage(SendMessageRequest request) {
        visibleMessages.add(new MessageContent(request.messageBody(), request.messageAttributes()));
        return SendMessageResponse.builder().build();
    }
    
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest request) {
        Integer waitTimeSeconds = request.waitTimeSeconds();
        long timeout = waitTimeSeconds != null ? waitTimeSeconds : 0; 
        
        try {
            MessageContent messageContent = visibleMessages.poll(timeout, TimeUnit.SECONDS);
            ReceiveMessageResponse result = ReceiveMessageResponse.builder().build();
            if (messageContent != null) {
                Message message = messageContent.toMessage();
                String receiptHandle = UUID.randomUUID().toString();
                inflight.put(receiptHandle, messageContent);
                message = message.toBuilder().receiptHandle(receiptHandle).build();
                result = result.toBuilder().messages(message).build();
            }
            return result;
        } catch (InterruptedException e) {
            // Imitate what the real SDK does
            throw SdkClientException.builder().cause(e).build();
        }
    }

    public ChangeMessageVisibilityResponse changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        String receiptHandle = request.receiptHandle();
        if (inflight.containsKey(receiptHandle)) {
            if (request.visibilityTimeout() == 0) {
                visibleMessages.add(inflight.remove(receiptHandle));
            } else {
                // TODO-RS: Message timers
            }
        } else {
            // TODO-RS: Error?
        }
        return ChangeMessageVisibilityResponse.builder().build();
    }

    public DeleteMessageResponse deleteMessage(DeleteMessageRequest request) {
        String receiptHandle = request.receiptHandle();
        if (inflight.remove(receiptHandle) == null) {
            // TODO-RS: Error? Or at least a hook so tests can
            // assert it actually succeeded?
        }
        return DeleteMessageResponse.builder().build();
    }
    
    public TagQueueResponse tagQueue(TagQueueRequest request) {
        tags.putAll(request.tags());
        return TagQueueResponse.builder().build();
    }
    
    public UntagQueueResponse untagQueue(UntagQueueRequest request) {
        request.tagKeys().forEach(tags.keySet()::remove);
        return UntagQueueResponse.builder().build();
    }
    
    public ListQueueTagsResponse listQueueTags() {
        return ListQueueTagsResponse.builder().tags(tags).build();
    }
}
