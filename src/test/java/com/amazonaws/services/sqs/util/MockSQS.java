package com.amazonaws.services.sqs.util;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueResponse;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsRequest;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsResponse;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.TagQueueRequest;
import software.amazon.awssdk.services.sqs.model.TagQueueResponse;
import software.amazon.awssdk.services.sqs.model.UntagQueueRequest;
import software.amazon.awssdk.services.sqs.model.UntagQueueResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockSQS implements SqsClient {
    private final String accountPrefix;
    private final Map<String, MockSQSQueue> queues = new HashMap<>();
    
    public MockSQS(String accountPrefix) {
        this.accountPrefix = accountPrefix;
    }
    
    private String getNameFromQueueUrl(String queueUrl) {
        if (!queueUrl.startsWith(accountPrefix)) {
            throw new IllegalArgumentException();
        }
        return queueUrl.substring(accountPrefix.length());
    }
    
    public MockSQSQueue getQueue(String queueUrl) {
        if (!queueUrl.startsWith(accountPrefix)) {
            throw new IllegalArgumentException();
        } else {
            String queueName = queueUrl.substring(accountPrefix.length());
            MockSQSQueue queue = queues.get(queueName);
            if (queue == null) {
                throw QueueDoesNotExistException.builder().message("The queue does not exist").build();
            }
            return queue;
        }
    }
    
    @Override
    public CreateQueueResponse createQueue(CreateQueueRequest request) {
        String queueName = request.queueName();
        String queueUrl = accountPrefix + queueName;
        queues.put(queueName, new MockSQSQueue(queueName));
        return CreateQueueResponse.builder().queueUrl(queueUrl).build();
    }

    @Override
    public ListQueuesResponse listQueues(ListQueuesRequest request) {
        String prefix = request.queueNamePrefix();
        String searchPrefix = prefix == null ? "" : prefix;
        List<String> queueUrls = queues.keySet().stream()
                .filter(name -> name.startsWith(searchPrefix))
                .map(name -> accountPrefix + name)
                .collect(Collectors.toList());
        return ListQueuesResponse.builder().queueUrls(queueUrls).build();
    }
    
    @Override
    public DeleteQueueResponse deleteQueue(DeleteQueueRequest request) {
        queues.remove(getQueue(request.queueUrl()).getQueueName());
        return DeleteQueueResponse.builder().build();
    }
    
    @Override
    public SendMessageResponse sendMessage(SendMessageRequest request) {
        return getQueue(request.queueUrl()).sendMessage(request);
    }
    
    @Override
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest request) {
        return getQueue(request.queueUrl()).receiveMessage(request);
    }
    
    @Override
    public ChangeMessageVisibilityResponse changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        return getQueue(request.queueUrl()).changeMessageVisibility(request);
    }
    
    @Override
    public DeleteMessageResponse deleteMessage(DeleteMessageRequest request) {
        return getQueue(request.queueUrl()).deleteMessage(request);
    }
    
    @Override
    public TagQueueResponse tagQueue(TagQueueRequest request) {
        return getQueue(request.queueUrl()).tagQueue(request);
    }
    
    @Override
    public UntagQueueResponse untagQueue(UntagQueueRequest request) {
        return getQueue(request.queueUrl()).untagQueue(request);
    }
    
    @Override
    public ListQueueTagsResponse listQueueTags(ListQueueTagsRequest request) {
        return getQueue(request.queueUrl()).listQueueTags();
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {

    }
}