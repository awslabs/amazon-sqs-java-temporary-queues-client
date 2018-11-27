package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.SQSQueueUtils;

/**
 * An AmazonSQS wrapper that only creates virtual, automatically-deleted queues.
 */
class AmazonSQSTemporaryQueuesClient extends AbstractAmazonSQSClientWrapper {

    private static final String HOST_QUEUE_NAME_PREFIX = "__HostQueue";
    
    private final ConcurrentMap<Map<String, String>, String> hostQueueUrls = new ConcurrentHashMap<>();

    private final String prefix;

    AmazonSQSTemporaryQueuesClient(AmazonSQS sqs, String queueNamePrefix) {
        super(sqs);
        this.prefix = queueNamePrefix;
    }

    public static AmazonSQS makeWrappedClient(AmazonSQS sqs, String queueNamePrefix) {
        AmazonSQS deleter = new AmazonSQSIdleQueueDeletingClient(sqs, HOST_QUEUE_NAME_PREFIX);
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
        return amazonSqsToBeExtended.createQueue(createVirtualQueueRequest);
    }

    @Override
    public void shutdown() {
        try {
            hostQueueUrls.values().forEach(amazonSqsToBeExtended::deleteQueue);
        } finally {
            super.shutdown();
        }
    }
}
