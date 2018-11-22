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
    
    AmazonSQSTemporaryQueuesClient(AmazonSQS sqs) {
        // TODO-RS: Be smarter about this: include host name, etc.
        this(sqs, UUID.randomUUID().toString());
    }

    AmazonSQSTemporaryQueuesClient(AmazonSQS sqs, String clientId) {
        super(sqs);
        this.prefix = HOST_QUEUE_NAME_PREFIX + "_" + clientId + "_";
    }
    
    static AmazonSQSTemporaryQueuesClient make(AmazonSQS sqs) {
        return new AmazonSQSTemporaryQueuesClient(makeWrappedClient(sqs));
    }
    
    static AmazonSQSTemporaryQueuesClient make(AmazonSQS sqs, String clientId) {
        return new AmazonSQSTemporaryQueuesClient(makeWrappedClient(sqs), clientId);
    }
    
    private static AmazonSQS makeWrappedClient(AmazonSQS sqs) {
        // TODO-RS: Determine the right strategy for naming the sweeping queue.
        // It needs to be shared between different clients, but testing friendly!
        // TODO-RS: Configure a tight MessageRetentionPeriod! Put explicit thought
        // into other configuration as well.
        CreateQueueRequest request = new CreateQueueRequest()
                .withQueueName(HOST_QUEUE_NAME_PREFIX + "Sweeper")
                // Server-side encryption is important here because we're putting
                // queue URLs into this queue.
                .addAttributesEntry(QueueAttributeName.KmsMasterKeyId.toString(), "alias/aws/sqs");
        String sweepingQueueUrl = sqs.createQueue(request).getQueueUrl();
        AmazonSQS deleter = new AmazonSQSIdleQueueDeletingClient(new AmazonSQSResponsesClient(sqs), HOST_QUEUE_NAME_PREFIX, sweepingQueueUrl);
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
            amazonSqsToBeExtended.shutdown();
        }
    }
}
