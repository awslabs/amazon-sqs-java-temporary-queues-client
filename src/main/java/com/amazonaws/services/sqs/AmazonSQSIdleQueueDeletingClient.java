package com.amazonaws.services.sqs;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;
import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.DaemonThreadFactory;
import com.amazonaws.services.sqs.util.ReceiveQueueBuffer;

class AmazonSQSIdleQueueDeletingClient extends AbstractAmazonSQSClientWrapper {

    private static final Log LOG = LogFactory.getLog(AmazonSQSIdleQueueDeletingClient.class);

    // Publicly visible constant
    public static final String IDLE_QUEUE_RETENTION_PERIOD = "IdleQueueRetentionPeriodSeconds";

    // TODO-RS: Configuration
    private static final long HEARTBEAT_INTERVAL_SECONDS = 5;
    private static final long IDLE_QUEUE_SWEEPER_PASS_SECONDS = 10;

    static final String LAST_HEARTBEAT_TIMESTAMP = "__AmazonSQSIdleQueueDeletingClient.LastHeartbeatTimestamp";

    private class QueueMetadata {
        private final String name;
        private Map<String, String> attributes;
        private Long heartbeatTimestamp;
        private Future<?> heartbeater;
        private ReceiveQueueBuffer buffer;

        private QueueMetadata(String name, String queueUrl, Map<String, String> attributes) {
            this.name = name;
            this.attributes = attributes;
            this.buffer = new ReceiveQueueBuffer(AmazonSQSIdleQueueDeletingClient.this, queueUrl);
        }
    }

    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
            new DaemonThreadFactory("AmazonSQSIdleQueueDeletingClient"));

    private final String queueNamePrefix;

    private final Map<String, QueueMetadata> queues = new ConcurrentHashMap<>();

    private final IdleQueueSweeper idleQueueSweeper;

    public AmazonSQSIdleQueueDeletingClient(AmazonSQSWithResponses sqs, String queueNamePrefix, String rootQueueUrl) {
        super(sqs.getAmazonSQS());
        if (queueNamePrefix.isEmpty()) {
            throw new IllegalArgumentException("Queue name prefix must be non-empty");
        }
        this.queueNamePrefix = queueNamePrefix;
        this.idleQueueSweeper = new IdleQueueSweeper(sqs, rootQueueUrl, queueNamePrefix,
                IDLE_QUEUE_SWEEPER_PASS_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public CreateQueueResult createQueue(CreateQueueRequest request) {
        if (!request.getAttributes().containsKey(IDLE_QUEUE_RETENTION_PERIOD)) {
            return super.createQueue(request);
        }

        Map<String, String> attributes = new HashMap<>(request.getAttributes());
        String retentionPeriod = attributes.remove(IDLE_QUEUE_RETENTION_PERIOD);
        String queueName = request.getQueueName();
        if (!queueName.startsWith(queueNamePrefix)) {
            throw new IllegalArgumentException();
        }

        CreateQueueRequest superRequest = request.clone()
                .withQueueName(queueName)
                .withAttributes(attributes);

        CreateQueueResult result = super.createQueue(superRequest);
        String queueUrl = result.getQueueUrl();

        amazonSqsToBeExtended.tagQueue(queueUrl,
                Collections.singletonMap(IDLE_QUEUE_RETENTION_PERIOD, retentionPeriod));

        // TODO-RS: Filter more carefully to all attributes valid for createQueue 
        Map<String, String> createdAttributes = amazonSqsToBeExtended.getQueueAttributes(queueUrl,
                Arrays.asList(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(),
                        QueueAttributeName.VisibilityTimeout.toString()))
                        .getAttributes();

        QueueMetadata metadata = new QueueMetadata(queueName, queueUrl, createdAttributes);
        queues.put(queueUrl, metadata);

        metadata.heartbeater = executor.scheduleAtFixedRate(() -> heartbeatToQueue(queueUrl), 
                0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);

        return result;
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
    }

    private void heartbeatToQueue(String queueUrl) {
        // TODO-RS: Clock drift? Shouldn't realistically be a problem as long as the idleness threshold is long enough.
        long currentTimestamp = System.currentTimeMillis();
        try {
            amazonSqsToBeExtended.tagQueue(queueUrl, 
                    Collections.singletonMap(LAST_HEARTBEAT_TIMESTAMP, String.valueOf(currentTimestamp)));
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
            if (lastHeartbeat == null || (System.currentTimeMillis() - lastHeartbeat) > 2 * HEARTBEAT_INTERVAL_SECONDS) {
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
                LOG.warn("Queue " + queueUrl + " successfully recreated.");
                return queueUrl;
            } catch (QueueDeletedRecentlyException e) {
                // Ignore, will retry later
                LOG.warn("Queue " + queueUrl + " was recently deleted, cannot create it yet.");
            }
        }

        String alternateQueueUrl = alternateQueueName(queueUrl);
        QueueMetadata metadata = queues.get(alternateQueueUrl);
        if (metadata == null && queue != null) {
            LOG.warn("Attempting to create failover queue: " + alternateQueueUrl);
            try {
                createQueue(new CreateQueueRequest().withQueueName(alternateQueueName(queue.name))
                        .withAttributes(queue.attributes));
                LOG.warn("Failover queue " + alternateQueueUrl + " successfully recreated.");
            } catch (QueueDeletedRecentlyException e) {
                // Ignore, will retry later
                LOG.warn("Failover queue " + alternateQueueUrl + " was recently deleted, cannot create it yet.");
            }	
        }
        return alternateQueueUrl;
    }

    private String alternateQueueName(String prefix) {
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
        // Here we have to also fetch from the backup queue if we created it
        // TODO-RS: Need to decide to stop fetching from the backup queue at some point too!
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
            try {
                Future<ReceiveMessageResult> receiveFuture = buffer.receiveMessageAsync(request);
                return receiveFuture.get(request.getWaitTimeSeconds(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new ReceiveMessageResult();
            } catch (ExecutionException e) {
                throw (RuntimeException)e.getCause();
            } catch (TimeoutException e) {
                return new ReceiveMessageResult();
            }
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
            List<Message> messages = amazonSqsToBeExtended.receiveMessage(request).getMessages();
            return messages;
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
        super.shutdown();
    }
}
