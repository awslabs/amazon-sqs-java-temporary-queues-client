package com.amazonaws.services.sqs;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.SQSQueueUtils;

/**
 * An AmazonSQS wrapper that only creates virtual, automatically-deleted queues.
 */
class AmazonSQSTemporaryQueuesClient extends AbstractAmazonSQSClientWrapper {

    // TODO-RS: Expose configuration
    private final static String QUEUE_RETENTION_PERIOD_SECONDS = Long.toString(TimeUnit.MINUTES.toSeconds(5));
    
    // These clients are owned by this one, and need to be shutdown when this client is.
    private final AmazonSQSIdleQueueDeletingClient deleter;
    private final AmazonSQS virtualizer;
    
    private final ConcurrentMap<Map<String, String>, String> hostQueueUrls = new ConcurrentHashMap<>();

    private final String prefix;

    private AmazonSQSTemporaryQueuesClient(AmazonSQS virtualizer, AmazonSQSIdleQueueDeletingClient deleter, String queueNamePrefix) {
        super(virtualizer);
        this.virtualizer = virtualizer;
        this.deleter = deleter;
        this.prefix = queueNamePrefix + UUID.randomUUID().toString();
    }

    public static AmazonSQSTemporaryQueuesClient makeWrappedClient(AmazonSQS sqs, String queueNamePrefix) {
        AmazonSQSIdleQueueDeletingClient deleter = new AmazonSQSIdleQueueDeletingClient(sqs, queueNamePrefix);
        AmazonSQS virtualizer = new AmazonSQSVirtualQueuesClient(deleter);
        return new AmazonSQSTemporaryQueuesClient(virtualizer, deleter, queueNamePrefix);
    }

    public void startIdleQueueSweeper(AmazonSQSRequesterClient requester, AmazonSQSResponderClient responder) {
        // TODO-RS: Allow configuration of the sweeping period?
        deleter.startSweeper(requester, responder, 5, TimeUnit.MINUTES, SQSQueueUtils.DEFAULT_EXCEPTION_HANDLER);
    }
    
    @Override
    public CreateQueueResult createQueue(CreateQueueRequest request) {
        String hostQueueUrl = hostQueueUrls.computeIfAbsent(request.getAttributes(), attributes -> {
            String name = prefix + hostQueueUrls.size();
            return amazonSqsToBeExtended.createQueue(request.withQueueName(name)).getQueueUrl();
        });

        Map<String, String> queueAttributes = new HashMap<>();
        queueAttributes.put(AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl);
        queueAttributes.put(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, QUEUE_RETENTION_PERIOD_SECONDS);
        
        CreateQueueRequest createVirtualQueueRequest = SQSQueueUtils.copyWithExtraAttributes(request, queueAttributes);
        return amazonSqsToBeExtended.createQueue(createVirtualQueueRequest);
    }

    @Override
    public void shutdown() {
        hostQueueUrls.values().forEach(amazonSqsToBeExtended::deleteQueue);
        virtualizer.shutdown();
        deleter.shutdown();
    }
}
