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

import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.Constants;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

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

    // We don't necessary support all queue attributes - some will behave differently on a virtual queue
    // In particular, a virtual FIFO queue will deduplicate at the scope of its host queue!
    private final static Set<String> SUPPORTED_QUEUE_ATTRIBUTES = new HashSet<>(Arrays.asList(
            QueueAttributeName.DELAY_SECONDS.name(),
            QueueAttributeName.MAXIMUM_MESSAGE_SIZE.name(),
            QueueAttributeName.MESSAGE_RETENTION_PERIOD.name(),
            QueueAttributeName.POLICY.name(),
            QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS.name(),
            QueueAttributeName.REDRIVE_POLICY.name(),
            QueueAttributeName.VISIBILITY_TIMEOUT.name(),
            QueueAttributeName.KMS_MASTER_KEY_ID.name(),
            QueueAttributeName.KMS_DATA_KEY_REUSE_PERIOD_SECONDS.name()));

    // These clients are owned by this one, and need to be shutdown when this client is.
    private final AmazonSQSIdleQueueDeletingClient deleter;
    private final SqsClient virtualizer;
    
    private final ConcurrentMap<Map<String, String>, String> hostQueueUrls = new ConcurrentHashMap<>();

    private final String prefix;
    private final long idleQueueRetentionPeriodSeconds;

    private AmazonSQSRequester requester;

    private AmazonSQSTemporaryQueuesClient(SqsClient virtualizer, AmazonSQSIdleQueueDeletingClient deleter, String queueNamePrefix, Long idleQueueRetentionPeriodSeconds) {
        super(virtualizer);
        this.virtualizer = virtualizer;
        this.deleter = deleter;
        this.prefix = queueNamePrefix + UUID.randomUUID().toString();

        if (idleQueueRetentionPeriodSeconds != null) {
            AmazonSQSIdleQueueDeletingClient.checkQueueRetentionPeriodBounds(idleQueueRetentionPeriodSeconds);
            this.idleQueueRetentionPeriodSeconds = idleQueueRetentionPeriodSeconds;
        } else {
            this.idleQueueRetentionPeriodSeconds = AmazonSQSTemporaryQueuesClientBuilder.IDLE_QUEUE_RETENTION_PERIOD_SECONDS_DEFAULT;
        }
    }
    
    private AmazonSQSTemporaryQueuesClient(SqsClient virtualizer, AmazonSQSIdleQueueDeletingClient deleter, String queueNamePrefix) {
        this(virtualizer, deleter, queueNamePrefix, null);
    }

    public static AmazonSQSTemporaryQueuesClient make(AmazonSQSRequesterClientBuilder builder) {
        SqsClient sqs = builder.getAmazonSQS().orElseGet(SqsClient::create);
        AmazonSQSIdleQueueDeletingClient deleter = new AmazonSQSIdleQueueDeletingClient(sqs, builder.getInternalQueuePrefix(), builder.getQueueHeartbeatInterval());
        SqsClient virtualizer = AmazonSQSVirtualQueuesClientBuilder.standard()
                .withAmazonSQS(deleter)
                .withHeartbeatIntervalSeconds(builder.getQueueHeartbeatInterval())
                .build();
        AmazonSQSTemporaryQueuesClient temporaryQueuesClient = new AmazonSQSTemporaryQueuesClient(virtualizer, deleter, builder.getInternalQueuePrefix(), builder.getIdleQueueRetentionPeriodSeconds());
        AmazonSQSRequesterClient requester = new AmazonSQSRequesterClient(temporaryQueuesClient, builder.getInternalQueuePrefix(), builder.getQueueAttributes());
        AmazonSQSResponderClient responder = new AmazonSQSResponderClient(temporaryQueuesClient);
        temporaryQueuesClient.startIdleQueueSweeper(requester, responder,
                builder.getIdleQueueSweepingPeriod(), builder.getIdleQueueSweepingTimeUnit());
        if (builder.getAmazonSQS().isPresent()) {
            requester.setShutdownHook(temporaryQueuesClient::close);
        } else {
            requester.setShutdownHook(() -> {
                temporaryQueuesClient.close();
                sqs.close();
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

    SqsClient getWrappedClient() {
        return virtualizer;
    }

    AmazonSQSRequester getRequester() {
        return requester;
    }
    
    @Override
    public CreateQueueResponse createQueue(CreateQueueRequest request) {
        // Check for unsupported queue attributes first
        Set<String> unsupportedQueueAttributes = new HashSet<>(request.attributesAsStrings().keySet());
        unsupportedQueueAttributes.removeAll(SUPPORTED_QUEUE_ATTRIBUTES);
        if (!unsupportedQueueAttributes.isEmpty()) {
            throw new IllegalArgumentException("Cannot create a temporary queue with the following attributes: "
                    + String.join(", ", unsupportedQueueAttributes));
        }

        Map<String, String> extraQueueAttributes = new HashMap<>();
        // Add the retention period to both the host queue and each virtual queue
        extraQueueAttributes.put(Constants.IDLE_QUEUE_RETENTION_PERIOD, Long.toString(idleQueueRetentionPeriodSeconds));
        String hostQueueUrl = hostQueueUrls.computeIfAbsent(request.attributesAsStrings(), attributes -> {
            CreateQueueRequest hostQueueCreateRequest = SQSQueueUtils.copyWithExtraAttributes(request, extraQueueAttributes);
            hostQueueCreateRequest = hostQueueCreateRequest.toBuilder().queueName(prefix + '-' + hostQueueUrls.size()).build();
            return amazonSqsToBeExtended.createQueue(hostQueueCreateRequest).queueUrl();
        });

        extraQueueAttributes.put(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl);
        // The host queue takes care of all the other queue attributes, so don't specify them when creating the virtual
        // queue or else the client may think we're trying to set them independently!
        CreateQueueRequest createVirtualQueueRequest = CreateQueueRequest.builder()
                .queueName(request.queueName())
                .attributesWithStrings(extraQueueAttributes).build();
        return amazonSqsToBeExtended.createQueue(createVirtualQueueRequest);
    }

    @Override
    public void close() {
        hostQueueUrls.values().forEach(
                url -> amazonSqsToBeExtended.deleteQueue(DeleteQueueRequest.builder().queueUrl(url).build())
        );
        virtualizer.close();
        deleter.close();
    }

    public void teardown() {
        deleter.teardown();
    }
}
