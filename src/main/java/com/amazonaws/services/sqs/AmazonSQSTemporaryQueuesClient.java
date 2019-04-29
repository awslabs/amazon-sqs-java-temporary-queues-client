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
 * <p>
 * This client is built on the functionality of the {@link AmazonSQSIdleQueueDeletingClient}
 * and the {@link AmazonSQSVirtualQueuesClient}, and is intended to be a drop-in replacement
 * for the AmazonSQS interface in cases where applications need to create many short-lived queues.
 * <p>
 * It automatically hosts all queues created with the same set of queue attributes on a single
 * SQS host queue. Both the host queues and virtual queues will have their "IdleQueueRetentionPeriodSeconds"
 * attribute set to 5 minutes.
 */
// TODO-RS: Rename this, as it's not what the AmazonSQSTemporaryQueuesClientBuilder is building!
// The latter supports creating temporary queues, but this class automatically creates ONLY temporary
// queues.
class AmazonSQSTemporaryQueuesClient extends AbstractAmazonSQSClientWrapper {

    // TODO-RS: Expose configuration
    private final static String QUEUE_RETENTION_PERIOD_SECONDS = Long.toString(TimeUnit.MINUTES.toSeconds(5));
    
    // These clients are owned by this one, and need to be shutdown when this client is.
    private final AmazonSQSIdleQueueDeletingClient deleter;
    private final AmazonSQS virtualizer;
    
    private final ConcurrentMap<Map<String, String>, String> hostQueueUrls = new ConcurrentHashMap<>();

    private final String prefix;

    private AmazonSQSRequester requester;
    
    private AmazonSQSTemporaryQueuesClient(AmazonSQS virtualizer, AmazonSQSIdleQueueDeletingClient deleter, String queueNamePrefix) {
        super(virtualizer);
        this.virtualizer = virtualizer;
        this.deleter = deleter;
        this.prefix = queueNamePrefix + UUID.randomUUID().toString();
    }

    public static AmazonSQSTemporaryQueuesClient make(AmazonSQSRequesterClientBuilder builder) {
        AmazonSQS sqs = builder.getAmazonSQS().orElseGet(AmazonSQSClientBuilder::defaultClient);
        AmazonSQSIdleQueueDeletingClient deleter = new AmazonSQSIdleQueueDeletingClient(sqs, builder.getInternalQueuePrefix());
        AmazonSQS virtualizer = new AmazonSQSVirtualQueuesClient(deleter);
        AmazonSQSTemporaryQueuesClient temporaryQueuesClient = new AmazonSQSTemporaryQueuesClient(virtualizer, deleter, builder.getInternalQueuePrefix());
        AmazonSQSRequesterClient requester = new AmazonSQSRequesterClient(temporaryQueuesClient, builder.getInternalQueuePrefix(), builder.getQueueAttributes());
        AmazonSQSResponderClient responder = new AmazonSQSResponderClient(temporaryQueuesClient);
        temporaryQueuesClient.startIdleQueueSweeper(requester, responder,
                builder.getIdleQueueSweepingPeriod(), builder.getIdleQueueSweepingTimeUnit());
        if (builder.getAmazonSQS().isPresent()) {
            requester.setShutdownHook(temporaryQueuesClient::shutdown);
        } else {
            requester.setShutdownHook(() -> {
                temporaryQueuesClient.shutdown();
                sqs.shutdown();
            });
        }
        return temporaryQueuesClient;
    }

    public void startIdleQueueSweeper(AmazonSQSRequesterClient requester, AmazonSQSResponderClient responder, int period, TimeUnit unit) {
        this.requester = requester;
        if (period > 0) {
            deleter.startSweeper(requester, responder, period, unit, SQSQueueUtils.DEFAULT_EXCEPTION_HANDLER);
        }
    }

    AmazonSQS getWrappedClient() {
        return virtualizer;
    }

    AmazonSQSRequester getRequester() {
        return requester;
    }
    
    @Override
    public CreateQueueResult createQueue(CreateQueueRequest request) {
        Map<String, String> extraQueueAttributes = new HashMap<>();
        // Add the retention period to both the host queue and each virtual queue
        extraQueueAttributes.put(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, QUEUE_RETENTION_PERIOD_SECONDS);
        String hostQueueUrl = hostQueueUrls.computeIfAbsent(request.getAttributes(), attributes -> {
            CreateQueueRequest hostQueueCreateRequest = SQSQueueUtils.copyWithExtraAttributes(request, extraQueueAttributes);
            hostQueueCreateRequest.setQueueName(prefix + '-' + hostQueueUrls.size());
            return amazonSqsToBeExtended.createQueue(hostQueueCreateRequest).getQueueUrl();
        });

        extraQueueAttributes.put(AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl);
        CreateQueueRequest createVirtualQueueRequest = SQSQueueUtils.copyWithExtraAttributes(request, extraQueueAttributes);
        return amazonSqsToBeExtended.createQueue(createVirtualQueueRequest);
    }

    @Override
    public void shutdown() {
        hostQueueUrls.values().forEach(amazonSqsToBeExtended::deleteQueue);
        virtualizer.shutdown();
        deleter.shutdown();
    }
}
