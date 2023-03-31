package com.amazonaws.services.sqs.util;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.MessageContent;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.ListQueueTagsResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.TagQueueRequest;
import com.amazonaws.services.sqs.model.TagQueueResult;
import com.amazonaws.services.sqs.model.UntagQueueRequest;
import com.amazonaws.services.sqs.model.UntagQueueResult;

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
    
    public SendMessageResult sendMessage(SendMessageRequest request) {
        visibleMessages.add(new MessageContent(request.getMessageBody(), request.getMessageAttributes()));
        return new SendMessageResult();    
    }
    
    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest request) {
        Integer waitTimeSeconds = request.getWaitTimeSeconds();
        long timeout = waitTimeSeconds != null ? waitTimeSeconds : 0; 
        
        try {
            MessageContent messageContent = visibleMessages.poll(timeout, TimeUnit.SECONDS);
            ReceiveMessageResult result = new ReceiveMessageResult();
            if (messageContent != null) {
                Message message = messageContent.toMessage();
                String receiptHandle = UUID.randomUUID().toString();
                inflight.put(receiptHandle, messageContent);
                message.withReceiptHandle(receiptHandle);
                result.withMessages(message);
            }
            return result;
        } catch (InterruptedException e) {
            // Imitate what the real SDK does
            throw new AmazonClientException(e);
        }
    }

    public ChangeMessageVisibilityResult changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        String receiptHandle = request.getReceiptHandle();
        if (inflight.containsKey(receiptHandle)) {
            if (request.getVisibilityTimeout() == 0) {
                visibleMessages.add(inflight.remove(receiptHandle));
            } else {
                // TODO-RS: Message timers
            }
        } else {
            // TODO-RS: Error?
        }
        return new ChangeMessageVisibilityResult();
    }

    public DeleteMessageResult deleteMessage(DeleteMessageRequest request) {
        String receiptHandle = request.getReceiptHandle();
        if (inflight.remove(receiptHandle) == null) {
            // TODO-RS: Error? Or at least a hook so tests can
            // assert it actually succeeded?
        }
        return new DeleteMessageResult();
    }
    
    public TagQueueResult tagQueue(TagQueueRequest request) {
        tags.putAll(request.getTags());
        return new TagQueueResult();
    }
    
    public UntagQueueResult untagQueue(UntagQueueRequest request) {
        request.getTagKeys().forEach(tags.keySet()::remove);
        return new UntagQueueResult();
    }
    
    public ListQueueTagsResult listQueueTags() {
        return new ListQueueTagsResult().withTags(tags);
    }
}
