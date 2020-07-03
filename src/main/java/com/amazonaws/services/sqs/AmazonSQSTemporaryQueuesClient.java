package com.amazonaws.services.sqs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
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

    // We don't necessary support all queue attributes - some will behave differently on a virtual queue
    // In particular, a virtual FIFO queue will deduplicate at the scope of its host queue!
    private final static Set<String> SUPPORTED_QUEUE_ATTRIBUTES = new HashSet<>(Arrays.asList(
            QueueAttributeName.DelaySeconds.name(),
            QueueAttributeName.MaximumMessageSize.name(),
            QueueAttributeName.MessageRetentionPeriod.name(),
            QueueAttributeName.Policy.name(),
            QueueAttributeName.ReceiveMessageWaitTimeSeconds.name(),
            QueueAttributeName.RedrivePolicy.name(),
            QueueAttributeName.VisibilityTimeout.name(),
            QueueAttributeName.KmsMasterKeyId.name(),
            QueueAttributeName.KmsDataKeyReusePeriodSeconds.name()));

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
        AmazonSQSIdleQueueDeletingClient deleter = new AmazonSQSIdleQueueDeletingClient(sqs, builder.getInternalQueuePrefix(), builder.getQueueHeartbeatInterval());
        AmazonSQS virtualizer = AmazonSQSVirtualQueuesClientBuilder.standard()
                .withAmazonSQS(deleter)
                .withHeartbeatIntervalSeconds(builder.getQueueHeartbeatInterval())
                .build();
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
        // Check for unsupported queue attributes first
        Set<String> unsupportedQueueAttributes = new HashSet<>(request.getAttributes().keySet());
        unsupportedQueueAttributes.removeAll(SUPPORTED_QUEUE_ATTRIBUTES);
        if (!unsupportedQueueAttributes.isEmpty()) {
            throw new IllegalArgumentException("Cannot create a temporary queue with the following attributes: "
                    + String.join(", ", unsupportedQueueAttributes));
        }

        Map<String, String> extraQueueAttributes = new HashMap<>();
        // Add the retention period to both the host queue and each virtual queue
        extraQueueAttributes.put(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, QUEUE_RETENTION_PERIOD_SECONDS);
        String hostQueueUrl = hostQueueUrls.computeIfAbsent(request.getAttributes(), attributes -> {
            CreateQueueRequest hostQueueCreateRequest = SQSQueueUtils.copyWithExtraAttributes(request, extraQueueAttributes);
            hostQueueCreateRequest.setQueueName(prefix + '-' + hostQueueUrls.size());
            return amazonSqsToBeExtended.createQueue(hostQueueCreateRequest).getQueueUrl();
        });

        extraQueueAttributes.put(AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl);
        // The host queue takes care of all the other queue attributes, so don't specify them when creating the virtual
        // queue or else the client may think we're trying to set them independently!
        CreateQueueRequest createVirtualQueueRequest = new CreateQueueRequest()
                .withQueueName(request.getQueueName())
                .withAttributes(extraQueueAttributes);
        return amazonSqsToBeExtended.createQueue(createVirtualQueueRequest);
    }

    @Override
    public void shutdown() {
        hostQueueUrls.values().forEach(amazonSqsToBeExtended::deleteQueue);
        virtualizer.shutdown();
        deleter.shutdown();
    }

    public void teardown() {
        deleter.teardown();
    }
}
