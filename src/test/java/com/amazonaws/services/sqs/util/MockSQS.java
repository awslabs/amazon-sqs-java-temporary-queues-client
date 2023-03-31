package com.amazonaws.services.sqs.util;

import com.amazonaws.services.sqs.AbstractAmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.ListQueueTagsRequest;
import com.amazonaws.services.sqs.model.ListQueueTagsResult;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.TagQueueRequest;
import com.amazonaws.services.sqs.model.TagQueueResult;
import com.amazonaws.services.sqs.model.UntagQueueRequest;
import com.amazonaws.services.sqs.model.UntagQueueResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MockSQS extends AbstractAmazonSQS {
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
                throw new QueueDoesNotExistException("The queue does not exist");
            }
            return queue;
        }
    }
    
    @Override
    public CreateQueueResult createQueue(CreateQueueRequest request) {
        String queueName = request.getQueueName();
        String queueUrl = accountPrefix + queueName;
        queues.put(queueName, new MockSQSQueue(queueName));
        return new CreateQueueResult().withQueueUrl(queueUrl);
    }

    @Override
    public ListQueuesResult listQueues(ListQueuesRequest request) {
        String prefix = request.getQueueNamePrefix();
        String searchPrefix = prefix == null ? "" : prefix;
        List<String> queueUrls = queues.keySet().stream()
                .filter(name -> name.startsWith(searchPrefix))
                .map(name -> accountPrefix + name)
                .collect(Collectors.toList());
        return new ListQueuesResult().withQueueUrls(queueUrls);
    }
    
    @Override
    public DeleteQueueResult deleteQueue(DeleteQueueRequest request) {
        queues.remove(getQueue(request.getQueueUrl()).getQueueName());
        return new DeleteQueueResult();
    }
    
    @Override
    public SendMessageResult sendMessage(SendMessageRequest request) {
        return getQueue(request.getQueueUrl()).sendMessage(request);
    }
    
    @Override
    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest request) {
        return getQueue(request.getQueueUrl()).receiveMessage(request);
    }
    
    @Override
    public ChangeMessageVisibilityResult changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        return getQueue(request.getQueueUrl()).changeMessageVisibility(request);
    }
    
    @Override
    public DeleteMessageResult deleteMessage(DeleteMessageRequest request) {
        return getQueue(request.getQueueUrl()).deleteMessage(request);
    }
    
    @Override
    public TagQueueResult tagQueue(TagQueueRequest request) {
        return getQueue(request.getQueueUrl()).tagQueue(request);
    }
    
    @Override
    public UntagQueueResult untagQueue(UntagQueueRequest request) {
        return getQueue(request.getQueueUrl()).untagQueue(request);
    }
    
    @Override
    public ListQueueTagsResult listQueueTags(ListQueueTagsRequest request) {
        return getQueue(request.getQueueUrl()).listQueueTags();
    }
}