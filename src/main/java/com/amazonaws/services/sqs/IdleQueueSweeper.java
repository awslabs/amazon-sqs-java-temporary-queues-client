package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.executors.DeduplicatedRunnable.deduplicated;
import static com.amazonaws.services.sqs.util.SQSQueueUtils.SQS_LIST_QUEUES_LIMIT;
import static com.amazonaws.services.sqs.util.SQSQueueUtils.forEachQueue;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.sqs.executors.SQSScheduledExecutorService;
import com.amazonaws.services.sqs.executors.SerializableReference;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.util.SQSQueueUtils;

@SuppressWarnings("squid:S2055") // SonarLint exception for the custom serialization approach.
class IdleQueueSweeper extends SQSScheduledExecutorService implements Serializable {

    private static final Log LOG = LogFactory.getLog(AmazonSQSIdleQueueDeletingClient.class);
    
    private final ConcurrentMap<String, Object> localScope = new ConcurrentHashMap<>();
    private final SerializableReference<IdleQueueSweeper> thisReference;

    public IdleQueueSweeper(AmazonSQSRequester sqsRequester, AmazonSQSResponder sqsResponder, String queueUrl, String queueNamePrefix, long period, TimeUnit unit) {
        super(sqsRequester, sqsResponder, queueUrl);

        thisReference = new SerializableReference<>(queueUrl, this, localScope);

        // Jitter the startup times to avoid throttling on tagging as much as possible.
        long initialDelay = ThreadLocalRandom.current().nextLong(period);

        // TODO-RS: Invoke this repeatedly over time to ensure the task is reset
        // if it ever dies for any reason.
        repeatAtFixedRate(deduplicated(() -> checkQueuesForIdleness(queueNamePrefix)), 
                initialDelay, period, unit);
    }

    protected void checkQueuesForIdleness(String prefix) {
        try {
            forEachQueue(this, sqs, prefix, SQS_LIST_QUEUES_LIMIT, (Serializable & Consumer<String>)this::checkQueueForIdleness);
        } catch (Exception e) {
            // Make sure the recurring task never throws so it doesn't terminate.
            LOG.error("Encounted error when checking queues for idleness (prefix = " + prefix + ")", e);
        }
    }

    protected void checkQueueForIdleness(String queueUrl) {
        LOG.info("Checking queue for idleness: " + queueUrl);
        try {
            if (isQueueIdle(queueUrl) && SQSQueueUtils.isQueueEmpty(sqs, queueUrl)) {
                LOG.info("Deleting idle queue: " + queueUrl);
                sqs.deleteQueue(queueUrl);
            }
        } catch (QueueDoesNotExistException e) {
            // Queue already deleted so nothing to do.
        }
    }

    private boolean isQueueIdle(String queueUrl) {
        Map<String, String> queueTags = sqs.listQueueTags(queueUrl).getTags();
        Long lastHeartbeat = AmazonSQSIdleQueueDeletingClient.getLongTag(queueTags, AmazonSQSIdleQueueDeletingClient.LAST_HEARTBEAT_TIMESTAMP_TAG);
        Long idleQueueRetentionPeriod = AmazonSQSIdleQueueDeletingClient.getLongTag(queueTags, AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD_TAG);
        long currentTimestamp = System.currentTimeMillis();

        return idleQueueRetentionPeriod != null && 
                (lastHeartbeat == null || 
                (currentTimestamp - lastHeartbeat) > idleQueueRetentionPeriod * 1000);
    }
    
    @Override
    protected SQSFutureTask<?> deserializeTask(Message message) {
        return thisReference.withScope(localScope, () -> super.deserializeTask(message));
    }
    
    protected Object writeReplace() throws ObjectStreamException {
        return thisReference;
    }
    
    @Override
    public void shutdown() {
        super.shutdown();
        try {
            awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Timed out waiting for IdleQueueSweeper to shut down");
        }
        thisReference.close();
    }
}