package com.amazonaws.services.sqs;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.amazonaws.services.sqs.model.QueueNameExistsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.QueueDeletedRecentlyException;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiptHandleIsInvalidException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.DaemonThreadFactory;
import com.amazonaws.services.sqs.util.ReceiveQueueBuffer;
import com.amazonaws.services.sqs.util.SQSQueueUtils;

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

    // Publicly visible constants
    public static final String IDLE_QUEUE_RETENTION_PERIOD = "IdleQueueRetentionPeriodSeconds";
    public static final long MINIMUM_IDLE_QUEUE_RETENTION_PERIOD_SECONDS = 1;
    public static final long MAXIMUM_IDLE_QUEUE_RETENTION_PERIOD_SECONDS = TimeUnit.MINUTES.toSeconds(5);
    public static final long HEARTBEAT_INTERVAL_SECONDS_DEFAULT = 5;
    public static final long HEARTBEAT_INTERVAL_SECONDS_MIN_VALUE = 1;

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

    public AmazonSQSIdleQueueDeletingClient(AmazonSQS sqs, String queueNamePrefix, Long heartbeatIntervalSeconds) {
        super(sqs);
        
        if (queueNamePrefix.isEmpty()) {
            throw new IllegalArgumentException("Queue name prefix must be non-empty");
        }

        this.queueNamePrefix = queueNamePrefix;

        if (heartbeatIntervalSeconds != null) {
            if (heartbeatIntervalSeconds < HEARTBEAT_INTERVAL_SECONDS_MIN_VALUE) {
                throw new IllegalArgumentException("Heartbeat Interval Seconds: " +
                        heartbeatIntervalSeconds +
                        " must be equal to or bigger than " +
                        HEARTBEAT_INTERVAL_SECONDS_MIN_VALUE);
            }
            this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
        } else {
            this.heartbeatIntervalSeconds = HEARTBEAT_INTERVAL_SECONDS_DEFAULT;
        }
    }

    public AmazonSQSIdleQueueDeletingClient(AmazonSQS sqs, String queueNamePrefix) {
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
        dlqAttributes.put(QueueAttributeName.MessageRetentionPeriod.name(), Long.toString(DLQ_MESSAGE_RETENTION_PERIOD));
        deadLetterQueueUrl = createOrUpdateQueue(queueNamePrefix + SWEEPING_QUEUE_DLQ_SUFFIX, dlqAttributes);
        String deadLetterQueueArn = super.getQueueAttributes(deadLetterQueueUrl,
                Collections.singletonList(QueueAttributeName.QueueArn.name()))
                        .getAttributes().get(QueueAttributeName.QueueArn.name());

        Map<String, String> queueAttributes = new HashMap<>();
        // Server-side encryption is important here because we're putting
        // queue URLs into this queue.
        queueAttributes.put(QueueAttributeName.KmsMasterKeyId.toString(), "alias/aws/sqs");
        queueAttributes.put(QueueAttributeName.RedrivePolicy.toString(),
                "{\"maxReceiveCount\":\"5\", \"deadLetterTargetArn\":\"" + deadLetterQueueArn + "\"}");
        // TODO-RS: Configure a tight MessageRetentionPeriod! Put explicit thought
        // into other configuration as well.
        String sweepingQueueUrl = createOrUpdateQueue(queueNamePrefix, queueAttributes);

        this.idleQueueSweeper = new IdleQueueSweeper(requester, responder, sweepingQueueUrl, queueNamePrefix,
                period, unit, exceptionHandler);
    }

    private String createOrUpdateQueue(String name, Map<String, String> attributes) {
        try {
            return super.createQueue(new CreateQueueRequest()
                    .withQueueName(name)
                    .withAttributes(attributes)).getQueueUrl();
        } catch (QueueNameExistsException e) {
            String queueUrl = super.getQueueUrl(name).getQueueUrl();
            super.setQueueAttributes(new SetQueueAttributesRequest()
                    .withQueueUrl(queueUrl)
                    .withAttributes(attributes));
            return queueUrl;
        }
    }

    @Override
    public CreateQueueResult createQueue(CreateQueueRequest request) {
        Map<String, String> attributes = new HashMap<>(request.getAttributes());
        Optional<Long> retentionPeriod = getRetentionPeriod(attributes);
        if (!retentionPeriod.isPresent()) {
            return super.createQueue(request);
        }

        String queueName = request.getQueueName();
        if (!queueName.startsWith(queueNamePrefix)) {
            throw new IllegalArgumentException();
        }

        CreateQueueRequest superRequest = request.clone()
                .withQueueName(queueName)
                .withAttributes(attributes);

        CreateQueueResult result = super.createQueue(superRequest);
        String queueUrl = result.getQueueUrl();

        String retentionPeriodString = retentionPeriod.get().toString();
        amazonSqsToBeExtended.tagQueue(queueUrl,
                Collections.singletonMap(IDLE_QUEUE_RETENTION_PERIOD_TAG, retentionPeriodString));

        // TODO-RS: Filter more carefully to all attributes valid for createQueue 
        List<String> attributeNames = Arrays.asList(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(),
                                                    QueueAttributeName.VisibilityTimeout.toString());
        Map<String, String> createdAttributes = amazonSqsToBeExtended.getQueueAttributes(queueUrl, attributeNames).getAttributes();
        createdAttributes.put(IDLE_QUEUE_RETENTION_PERIOD, retentionPeriodString);

        QueueMetadata metadata = new QueueMetadata(queueName, queueUrl, createdAttributes);
        queues.put(queueUrl, metadata);

        metadata.heartbeater = executor.scheduleAtFixedRate(() -> heartbeatToQueue(queueUrl), 
                0, heartbeatIntervalSeconds, TimeUnit.SECONDS);

        return result;
    }

    static Optional<Long> getRetentionPeriod(Map<String, String> queueAttributes) {
        return Optional.ofNullable(queueAttributes.remove(IDLE_QUEUE_RETENTION_PERIOD))
                       .map(Long::parseLong)
                       .map(AmazonSQSIdleQueueDeletingClient::checkQueueRetentionPeriodBounds);
    }
    
    static long checkQueueRetentionPeriodBounds(long retentionPeriod) {
        if (retentionPeriod < MINIMUM_IDLE_QUEUE_RETENTION_PERIOD_SECONDS ||
                retentionPeriod > MAXIMUM_IDLE_QUEUE_RETENTION_PERIOD_SECONDS) {
            throw new IllegalArgumentException("The " + IDLE_QUEUE_RETENTION_PERIOD + 
                    " attribute must be between " + MINIMUM_IDLE_QUEUE_RETENTION_PERIOD_SECONDS +
                    " and " + MAXIMUM_IDLE_QUEUE_RETENTION_PERIOD_SECONDS + " seconds");
        }
        return retentionPeriod;
    }
    
    @Override
    public GetQueueAttributesResult getQueueAttributes(GetQueueAttributesRequest request) {
        QueueMetadata metadata = queues.get(request.getQueueUrl());
        if (metadata != null) {
            Map<String, String> filteredAttributes = new HashMap<>(metadata.attributes);
            filteredAttributes.keySet().retainAll(request.getAttributeNames());
            return new GetQueueAttributesResult().withAttributes(filteredAttributes);
        }

        return super.getQueueAttributes(request);
    }

    @Override
    public SetQueueAttributesResult setQueueAttributes(SetQueueAttributesRequest request) {
        SetQueueAttributesResult result = super.setQueueAttributes(request);

        QueueMetadata queue = queues.get(request.getQueueUrl());
        if (queue != null) {
            queue.attributes.putAll(request.getAttributes());
        }

        return result;
    }

    @Override
    public DeleteQueueResult deleteQueue(DeleteQueueRequest request) {
        DeleteQueueResult result = super.deleteQueue(request);
        queueDeleted(request.getQueueUrl());
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
            super.deleteQueue(alternateQueueUrl);
            alternateMetadata.heartbeater.cancel(true);
            alternateMetadata.buffer.shutdown();
        }
    }

    private void heartbeatToQueue(String queueUrl) {
        // TODO-RS: Clock drift? Shouldn't realistically be a problem as long as the idleness threshold is long enough.
        long currentTimestamp = System.currentTimeMillis();
        try {
            amazonSqsToBeExtended.tagQueue(queueUrl, 
                    Collections.singletonMap(LAST_HEARTBEAT_TIMESTAMP_TAG, String.valueOf(currentTimestamp)));
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
            if (lastHeartbeat != null && (System.currentTimeMillis() - lastHeartbeat) < 2 * heartbeatIntervalSeconds * 1000) {
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
                createQueue(new CreateQueueRequest().withQueueName(queue.name)
                        .withAttributes(queue.attributes));
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
                createQueue(new CreateQueueRequest().withQueueName(alternateQueueName(queue.name))
                        .withAttributes(queue.attributes));
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
    public SendMessageResult sendMessage(SendMessageRequest request) {
        try {
            heartbeatToQueueIfNecessary(request.getQueueUrl());
            return super.sendMessage(request);
        } catch (QueueDoesNotExistException e) {
            request.setQueueUrl(recreateQueue(request.getQueueUrl()));
            return super.sendMessage(request);
        }
    }

    @Override
    public SendMessageBatchResult sendMessageBatch(SendMessageBatchRequest request) {
        try {
            heartbeatToQueueIfNecessary(request.getQueueUrl());
            return super.sendMessageBatch(request);
        } catch (QueueDoesNotExistException e) {
            request.setQueueUrl(recreateQueue(request.getQueueUrl()));
            return super.sendMessageBatch(request);
        }
    }

    @Override
    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest request) {
        // Here we have to also fetch from the backup queue if we created it.
        String queueUrl = request.getQueueUrl();
        String alternateQueueUrl = alternateQueueName(queueUrl);
        QueueMetadata alternateMetadata = queues.get(alternateQueueUrl);
        if (alternateMetadata != null) {
            ReceiveQueueBuffer buffer = alternateMetadata.buffer;
            ReceiveMessageRequest alternateRequest = request.clone().withQueueUrl(alternateQueueUrl);
            buffer.submit(executor, () -> receiveIgnoringNonExistantQueue(request),
                    queueUrl, request.getVisibilityTimeout());
            buffer.submit(executor, () -> receiveIgnoringNonExistantQueue(alternateRequest),
                    queueUrl, request.getVisibilityTimeout());
            Future<ReceiveMessageResult> receiveFuture = buffer.receiveMessageAsync(request);
            return SQSQueueUtils.waitForFuture(receiveFuture);
        } else {
            try {
                heartbeatToQueueIfNecessary(queueUrl);
                return super.receiveMessage(request);
            } catch (QueueDoesNotExistException e) {
                request.setQueueUrl(recreateQueue(queueUrl));
                return super.receiveMessage(request);
            }
        }
    }

    private List<Message> receiveIgnoringNonExistantQueue(ReceiveMessageRequest request) {
        try {
            heartbeatToQueueIfNecessary(request.getQueueUrl());
            return amazonSqsToBeExtended.receiveMessage(request).getMessages();
        } catch (QueueDoesNotExistException e) {
            return Collections.emptyList();
        }
    }

    @Override
    public ChangeMessageVisibilityResult changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        // If the queue is deleted, there's no way to change the message visibility.
        try {
            return super.changeMessageVisibility(request);
        } catch (QueueDoesNotExistException|ReceiptHandleIsInvalidException e) {
            // Try on the alternate queue
            return super.changeMessageVisibility(
                    request.clone().withQueueUrl(alternateQueueName(request.getQueueUrl())));
        }
    }

    @Override
    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest request) {
        // If the queue is deleted, there's no way to change the message visibility.
        try {
            return super.changeMessageVisibilityBatch(request);
        } catch (QueueDoesNotExistException|ReceiptHandleIsInvalidException e) {
            // Try on the alternate queue
            ChangeMessageVisibilityBatchRequest alternateRequest = request.clone().withQueueUrl(alternateQueueName(request.getQueueUrl()));
            return super.changeMessageVisibilityBatch(alternateRequest);
        }
    }

    @Override
    public DeleteMessageResult deleteMessage(DeleteMessageRequest request) {
        String queueUrl = request.getQueueUrl();
        try {
            heartbeatToQueueIfNecessary(queueUrl);
            return super.deleteMessage(request);
        } catch (QueueDoesNotExistException|ReceiptHandleIsInvalidException e) {
            try {
                return super.deleteMessage(
                        request.clone().withQueueUrl(alternateQueueName(request.getQueueUrl())));
            } catch (QueueDoesNotExistException e2) {
                // Silently fail - the message is definitely deleted after all!
                return new DeleteMessageResult();
            }
        }
    }

    @Override
    public DeleteMessageBatchResult deleteMessageBatch(DeleteMessageBatchRequest request) {
        String queueUrl = request.getQueueUrl();
        try {
            heartbeatToQueueIfNecessary(queueUrl);
            return super.deleteMessageBatch(request);
        } catch (QueueDoesNotExistException e) {
            try {
                return super.deleteMessageBatch(
                        request.clone().withQueueUrl(alternateQueueName(request.getQueueUrl())));
            } catch (QueueDoesNotExistException e2) {
                // Silently fail - the message is definitely deleted after all!
                return new DeleteMessageBatchResult();
            }
        }
    }

    @Override
    public void shutdown() {
        if (idleQueueSweeper != null) {
            idleQueueSweeper.shutdown();
        }
        queues.values().forEach(metadata -> metadata.buffer.shutdown());
    }
    
    public void teardown() {
        shutdown();
        if (idleQueueSweeper != null) {
            amazonSqsToBeExtended.deleteQueue(idleQueueSweeper.getQueueUrl());
        }
        if (deadLetterQueueUrl != null) {
            amazonSqsToBeExtended.deleteQueue(deadLetterQueueUrl);
        }
    }
}
