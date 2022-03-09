package com.amazonaws.services.sqs;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Consumer;

import com.amazonaws.services.sqs.util.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.DaemonThreadFactory;
import com.amazonaws.services.sqs.util.ReceiveQueueBuffer;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDeletedRecentlyException;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.ReceiptHandleIsInvalidException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.TagQueueRequest;

/**
 * An AmazonSQS wrapper that adds automatically deletes unused queues after a configurable
 * period of inactivity.
 * <p>
 * This client monitors all queues created with the "IdleQueueRetentionPeriodSeconds" queue
 * attribute. Such queues must have names that begin with the prefix provided in the constructor,
 * as this client uses {@link #listQueues(ListQueuesRequest)} to sweep them.
 * <p>
 * This client uses a heartbeating mechanism based on queue tags. Making API calls to queues
 * through this client causes tags on those queues to be refreshed every 5 seconds (by default,
 * heartbeating mechanism is configurable). If the process
 * using a client shuts down uncleanly, other client instances using the same queue prefix will
 * detect that its queue(s) are idle and delete them.
 */
class AmazonSQSIdleQueueDeletingClient extends AbstractAmazonSQSClientWrapper {

    private static final Log LOG = LogFactory.getLog(AmazonSQSIdleQueueDeletingClient.class);

    static final String IDLE_QUEUE_RETENTION_PERIOD_TAG = "__IdleQueueRetentionPeriodSeconds";

    private static final String SWEEPING_QUEUE_DLQ_SUFFIX = "_DLQ";
    private static final long DLQ_MESSAGE_RETENTION_PERIOD = TimeUnit.DAYS.toSeconds(14);

    static final String LAST_HEARTBEAT_TIMESTAMP_TAG = "__AmazonSQSIdleQueueDeletingClient.LastHeartbeatTimestamp";

    private class QueueMetadata {
        private final String name;
        private Map<String, String> attributes;
        private Long heartbeatTimestamp;
        private Future<?> heartbeater;
        private ReceiveQueueBuffer buffer;

        private QueueMetadata(String name, String queueUrl, Map<String, String> attributes) {
            this.name = name;
            this.attributes = attributes;
            this.buffer = new ReceiveQueueBuffer(AmazonSQSIdleQueueDeletingClient.this, executor, queueUrl);
        }
    }

    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
            new DaemonThreadFactory("AmazonSQSIdleQueueDeletingClient"));

    private final String queueNamePrefix;
    private final long heartbeatIntervalSeconds;

    private final Map<String, QueueMetadata> queues = new ConcurrentHashMap<>();

    private IdleQueueSweeper idleQueueSweeper;
    private String deadLetterQueueUrl;

    public AmazonSQSIdleQueueDeletingClient(SqsClient sqs, String queueNamePrefix, Long heartbeatIntervalSeconds) {
        super(sqs);
        
        if (queueNamePrefix.isEmpty()) {
            throw new IllegalArgumentException("Queue name prefix must be non-empty");
        }

        this.queueNamePrefix = queueNamePrefix;

        if (heartbeatIntervalSeconds != null) {
            if (heartbeatIntervalSeconds < Constants.HEARTBEAT_INTERVAL_SECONDS_MIN_VALUE) {
                throw new IllegalArgumentException("Heartbeat Interval Seconds: " +
                        heartbeatIntervalSeconds +
                        " must be equal to or bigger than " +
                        Constants.HEARTBEAT_INTERVAL_SECONDS_MIN_VALUE);
            }
            this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
        } else {
            this.heartbeatIntervalSeconds = Constants.HEARTBEAT_INTERVAL_SECONDS_DEFAULT;
        }
    }

    public AmazonSQSIdleQueueDeletingClient(SqsClient sqs, String queueNamePrefix) {
        this(sqs, queueNamePrefix, null);
    }

    protected synchronized void startSweeper(AmazonSQSRequester requester, AmazonSQSResponder responder,
                                             long period, TimeUnit unit,
                                             Consumer<Exception> exceptionHandler) {
        if (this.idleQueueSweeper != null) {
            throw new IllegalStateException("Idle queue sweeper is already started!");
        }

        // Create the DLQ first so the primary queue can reference it
        // Note that SSE doesn't have to be enabled on this queue since the messages
        // will already be encrypted in the primary queue, and dead-lettering doesn't affect that.
        // The messages will still be receivable from the DLQ regardless.
        Map<String, String> dlqAttributes = new HashMap<>();
        dlqAttributes.put(QueueAttributeName.MESSAGE_RETENTION_PERIOD.toString(), Long.toString(DLQ_MESSAGE_RETENTION_PERIOD));
        deadLetterQueueUrl = createOrUpdateQueue(queueNamePrefix + SWEEPING_QUEUE_DLQ_SUFFIX, dlqAttributes);
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(deadLetterQueueUrl).attributeNamesWithStrings(QueueAttributeName.QUEUE_ARN.toString()).build();
        String deadLetterQueueArn = super.getQueueAttributes(getQueueAttributesRequest).attributesAsStrings().get(QueueAttributeName.QUEUE_ARN.toString());

        Map<String, String> queueAttributes = new HashMap<>();
        // Server-side encryption is important here because we're putting
        // queue URLs into this queue.
        queueAttributes.put(QueueAttributeName.KMS_MASTER_KEY_ID.toString(), "alias/aws/sqs");
        queueAttributes.put(QueueAttributeName.REDRIVE_POLICY.toString(),
                "{\"maxReceiveCount\":\"5\", \"deadLetterTargetArn\":\"" + deadLetterQueueArn + "\"}");
        // TODO-RS: Configure a tight MessageRetentionPeriod! Put explicit thought
        // into other configuration as well.
        String sweepingQueueUrl = createOrUpdateQueue(queueNamePrefix, queueAttributes);

        this.idleQueueSweeper = new IdleQueueSweeper(requester, responder, sweepingQueueUrl, queueNamePrefix,
                period, unit, exceptionHandler);
    }

    private String createOrUpdateQueue(String name, Map<String, String> attributes) {
        try {
            return super.createQueue(CreateQueueRequest.builder()
                    .queueName(name)
                    .attributesWithStrings(attributes).build()).queueUrl();
        } catch (QueueNameExistsException e) {
            String queueUrl = super.getQueueUrl(GetQueueUrlRequest.builder().queueName(name).build()).queueUrl();
            super.setQueueAttributes(SetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributesWithStrings(attributes).build());
            return queueUrl;
        }
    }

    @Override
    public CreateQueueResponse createQueue(CreateQueueRequest request) {
        Map<String, String> attributes = new HashMap<>(request.attributesAsStrings());
        Optional<Long> retentionPeriod = getRetentionPeriod(attributes);
        if (!retentionPeriod.isPresent()) {
            return super.createQueue(request);
        }

        String queueName = request.queueName();
        if (!queueName.startsWith(queueNamePrefix)) {
            throw new IllegalArgumentException();
        }

        String retentionPeriodString = retentionPeriod.get().toString();
        long currentTimestamp = System.currentTimeMillis();
        CreateQueueRequest superRequest = request.toBuilder().copy()
                .queueName(queueName)
                .attributesWithStrings(attributes).build();

        CreateQueueResponse result = super.createQueue(superRequest);
        String queueUrl = result.queueUrl();

        Map<String, String> tags = new HashMap<>();
        tags.put(IDLE_QUEUE_RETENTION_PERIOD_TAG, retentionPeriodString);
        tags.put(LAST_HEARTBEAT_TIMESTAMP_TAG, String.valueOf(currentTimestamp));
        TagQueueRequest.Builder tagQueueBuilder = TagQueueRequest.builder().queueUrl(queueUrl).tags(tags);
        amazonSqsToBeExtended.tagQueue(tagQueueBuilder.build());

        // TODO-RS: Filter more carefully to all attributes valid for createQueue 
        List<String> attributeNames = Arrays.asList(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS.toString(),
                                                    QueueAttributeName.VISIBILITY_TIMEOUT.toString());
        GetQueueAttributesRequest.Builder getQueueAttributesBuilder = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl).attributeNamesWithStrings(attributeNames);
        Map<String, String> createdAttributes = new HashMap<>(amazonSqsToBeExtended.getQueueAttributes(getQueueAttributesBuilder.build()).attributesAsStrings());
        createdAttributes.put(Constants.IDLE_QUEUE_RETENTION_PERIOD, retentionPeriodString);

        QueueMetadata metadata = new QueueMetadata(queueName, queueUrl, createdAttributes);
        queues.put(queueUrl, metadata);

        long initialDelay = ThreadLocalRandom.current().nextLong(heartbeatIntervalSeconds);
        metadata.heartbeater = executor.scheduleAtFixedRate(() -> heartbeatToQueue(queueUrl),
                initialDelay, heartbeatIntervalSeconds, TimeUnit.SECONDS);

        return result;
    }

    static Optional<Long> getRetentionPeriod(Map<String, String> queueAttributes) {
        return Optional.ofNullable(queueAttributes.remove(Constants.IDLE_QUEUE_RETENTION_PERIOD))
                       .map(Long::parseLong)
                       .map(AmazonSQSIdleQueueDeletingClient::checkQueueRetentionPeriodBounds);
    }
    
    static long checkQueueRetentionPeriodBounds(long retentionPeriod) {
        if (retentionPeriod < Constants.MINIMUM_IDLE_QUEUE_RETENTION_PERIOD_SECONDS) {
            throw new IllegalArgumentException("The " + Constants.IDLE_QUEUE_RETENTION_PERIOD +
                    " attribute bigger or equal to " + Constants.MINIMUM_IDLE_QUEUE_RETENTION_PERIOD_SECONDS + " seconds");
        }
        return retentionPeriod;
    }
    
    @Override
    public GetQueueAttributesResponse getQueueAttributes(GetQueueAttributesRequest request) {
        QueueMetadata metadata = queues.get(request.queueUrl());
        if (metadata != null) {
            Map<String, String> filteredAttributes = new HashMap<>(metadata.attributes);
            filteredAttributes.keySet().retainAll(request.attributeNamesAsStrings());
            return GetQueueAttributesResponse.builder().attributesWithStrings(filteredAttributes).build();
        }

        return super.getQueueAttributes(request);
    }

    @Override
    public SetQueueAttributesResponse setQueueAttributes(SetQueueAttributesRequest request) {
        SetQueueAttributesResponse result = super.setQueueAttributes(request);

        QueueMetadata queue = queues.get(request.queueUrl());
        if (queue != null) {
            queue.attributes.putAll(request.attributesAsStrings());
        }

        return result;
    }

    @Override
    public DeleteQueueResponse deleteQueue(DeleteQueueRequest request) {
        DeleteQueueResponse result = super.deleteQueue(request);
        queueDeleted(request.queueUrl());
        return result;
    }

    private void queueDeleted(String queueUrl) {
        QueueMetadata metadata = queues.remove(queueUrl);
        if (metadata != null && metadata.heartbeater != null) {
            metadata.heartbeater.cancel(true);
            metadata.buffer.shutdown();
        }

        String alternateQueueUrl = alternateQueueName(queueUrl);
        QueueMetadata alternateMetadata = queues.remove(alternateQueueUrl);
        if (alternateMetadata != null) {
            super.deleteQueue(DeleteQueueRequest.builder().queueUrl(alternateQueueUrl).build());
            alternateMetadata.heartbeater.cancel(true);
            alternateMetadata.buffer.shutdown();
        }
    }

    private void heartbeatToQueue(String queueUrl) {
        // TODO-RS: Clock drift? Shouldn't realistically be a problem as long as the idleness threshold is long enough.
        long currentTimestamp = System.currentTimeMillis();
        try {
            Map<String, String> tags = new HashMap<>();
            tags.put(LAST_HEARTBEAT_TIMESTAMP_TAG, String.valueOf(currentTimestamp));
            TagQueueRequest.Builder tagQueueBuilder = TagQueueRequest.builder()
                    .queueUrl(queueUrl).tags(tags);
            amazonSqsToBeExtended.tagQueue(tagQueueBuilder.build());
        } catch (QueueDoesNotExistException e) {
            recreateQueue(queueUrl);
            // TODO-RS: Retry right away
        }
        queues.get(queueUrl).heartbeatTimestamp = currentTimestamp;
    }

    private void heartbeatToQueueIfNecessary(String queueUrl) {
        QueueMetadata queueMetadata = queues.get(queueUrl);
        if (queueMetadata != null) {
            Long lastHeartbeat = queueMetadata.heartbeatTimestamp;
            if (lastHeartbeat != null && (System.currentTimeMillis() - lastHeartbeat) < heartbeatIntervalSeconds * 1000) {
                return;
            }
            heartbeatToQueue(queueUrl);
        }
    }

    static Long getLongTag(Map<String, String> queueTags, String key) {
        String tag = queueTags.get(key);
        return tag == null ? null : Long.parseLong(tag);
    }

    private String recreateQueue(String queueUrl) {
        // TODO-RS: CW metrics
        QueueMetadata queue = queues.get(queueUrl);
        if (queue != null) {
            LOG.warn("Queue " + queueUrl + " was deleted while it was still in use! Attempting to recreate...");
            try {
                createQueue(CreateQueueRequest.builder().queueName(queue.name)
                        .attributesWithStrings(queue.attributes).build());
                LOG.info("Queue " + queueUrl + " successfully recreated.");
                return queueUrl;
            } catch (QueueDeletedRecentlyException e) {
                // Ignore, will retry later
                LOG.warn("Queue " + queueUrl + " was recently deleted, cannot create it yet.");
            }
        }

        String alternateQueueUrl = alternateQueueName(queueUrl);
        QueueMetadata metadata = queues.get(alternateQueueUrl);
        if (metadata == null && queue != null) {
            LOG.info("Attempting to create failover queue: " + alternateQueueUrl);
            try {
                createQueue(CreateQueueRequest.builder().queueName(alternateQueueName(queue.name))
                        .attributesWithStrings(queue.attributes).build());
                LOG.info("Failover queue " + alternateQueueUrl + " successfully created.");
            } catch (QueueDeletedRecentlyException e) {
                // Ignore, will retry later
                LOG.warn("Failover queue " + alternateQueueUrl + " was recently deleted, cannot create it yet.");
            }	
        }
        return alternateQueueUrl;
    }

    static String alternateQueueName(String prefix) {
        return prefix + "-Failover";
    }

    @Override
    public SendMessageResponse sendMessage(SendMessageRequest request) {
        try {
            heartbeatToQueueIfNecessary(request.queueUrl());
            return super.sendMessage(request);
        } catch (QueueDoesNotExistException e) {
            SendMessageRequest newRequest = request.toBuilder().queueUrl(recreateQueue(request.queueUrl())).build();
            return super.sendMessage(newRequest);
        }
    }

    @Override
    public SendMessageBatchResponse sendMessageBatch(SendMessageBatchRequest request) {
        try {
            heartbeatToQueueIfNecessary(request.queueUrl());
            return super.sendMessageBatch(request);
        } catch (QueueDoesNotExistException e) {
            SendMessageBatchRequest newRequest = request.toBuilder().queueUrl(recreateQueue(request.queueUrl())).build();
            return super.sendMessageBatch(newRequest);
        }
    }

    @Override
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest request) {
        // Here we have to also fetch from the backup queue if we created it.
        String queueUrl = request.queueUrl();
        String alternateQueueUrl = alternateQueueName(queueUrl);
        QueueMetadata alternateMetadata = queues.get(alternateQueueUrl);
        if (alternateMetadata != null) {
            ReceiveQueueBuffer buffer = alternateMetadata.buffer;
            ReceiveMessageRequest alternateRequest = request.toBuilder().copy().queueUrl(alternateQueueUrl).build();
            buffer.submit(executor, () -> receiveIgnoringNonExistantQueue(request),
                    queueUrl, request.visibilityTimeout());
            buffer.submit(executor, () -> receiveIgnoringNonExistantQueue(alternateRequest),
                    queueUrl, request.visibilityTimeout());
            Future<ReceiveMessageResponse> receiveFuture = buffer.receiveMessageAsync(request);
            return SQSQueueUtils.waitForFuture(receiveFuture);
        } else {
            try {
                heartbeatToQueueIfNecessary(queueUrl);
                return super.receiveMessage(request);
            } catch (QueueDoesNotExistException e) {
                ReceiveMessageRequest newRequest = request.toBuilder().queueUrl(recreateQueue(queueUrl)).build();
                return super.receiveMessage(newRequest);
            }
        }
    }

    private List<Message> receiveIgnoringNonExistantQueue(ReceiveMessageRequest request) {
        try {
            heartbeatToQueueIfNecessary(request.queueUrl());
            return amazonSqsToBeExtended.receiveMessage(request).messages();
        } catch (QueueDoesNotExistException e) {
            return Collections.emptyList();
        }
    }

    @Override
    public ChangeMessageVisibilityResponse changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        // If the queue is deleted, there's no way to change the message visibility.
        try {
            return super.changeMessageVisibility(request);
        } catch (QueueDoesNotExistException|ReceiptHandleIsInvalidException e) {
            // Try on the alternate queue
            return super.changeMessageVisibility(
                    request.toBuilder().copy().queueUrl(alternateQueueName(request.queueUrl())).build());
        }
    }

    @Override
    public ChangeMessageVisibilityBatchResponse changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest request) {
        // If the queue is deleted, there's no way to change the message visibility.
        try {
            return super.changeMessageVisibilityBatch(request);
        } catch (QueueDoesNotExistException|ReceiptHandleIsInvalidException e) {
            // Try on the alternate queue
            ChangeMessageVisibilityBatchRequest alternateRequest = request.toBuilder().copy().queueUrl(alternateQueueName(request.queueUrl())).build();
            return super.changeMessageVisibilityBatch(alternateRequest);
        }
    }

    @Override
    public DeleteMessageResponse deleteMessage(DeleteMessageRequest request) {
        String queueUrl = request.queueUrl();
        try {
            heartbeatToQueueIfNecessary(queueUrl);
            return super.deleteMessage(request);
        } catch (QueueDoesNotExistException|ReceiptHandleIsInvalidException e) {
            try {
                return super.deleteMessage(
                        request.toBuilder().copy().queueUrl(alternateQueueName(request.queueUrl())).build());
            } catch (QueueDoesNotExistException e2) {
                // Silently fail - the message is definitely deleted after all!
                return DeleteMessageResponse.builder().build();
            }
        }
    }

    @Override
    public DeleteMessageBatchResponse deleteMessageBatch(DeleteMessageBatchRequest request) {
        String queueUrl = request.queueUrl();
        try {
            heartbeatToQueueIfNecessary(queueUrl);
            return super.deleteMessageBatch(request);
        } catch (QueueDoesNotExistException e) {
            try {
                return super.deleteMessageBatch(
                        request.toBuilder().copy().queueUrl(alternateQueueName(request.queueUrl())).build());
            } catch (QueueDoesNotExistException e2) {
                // Silently fail - the message is definitely deleted after all!
                return DeleteMessageBatchResponse.builder().build();
            }
        }
    }

    @Override
    public void close() {
        if (idleQueueSweeper != null) {
            idleQueueSweeper.shutdown();
        }
        queues.values().forEach(metadata -> metadata.buffer.shutdown());
    }
    
    public void teardown() {
        close();
        if (idleQueueSweeper != null) {
            amazonSqsToBeExtended.deleteQueue(DeleteQueueRequest.builder().queueUrl(idleQueueSweeper.getQueueUrl()).build());
        }
        if (deadLetterQueueUrl != null) {
            amazonSqsToBeExtended.deleteQueue(DeleteQueueRequest.builder().queueUrl(deadLetterQueueUrl).build());
        }
    }
}
