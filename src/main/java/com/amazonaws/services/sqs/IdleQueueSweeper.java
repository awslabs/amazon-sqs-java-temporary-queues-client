package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.executors.DeduplicatedRunnable.deduplicated;
import static com.amazonaws.services.sqs.executors.SerializableFunction.serializable;
import static com.amazonaws.services.sqs.util.SQSQueueUtils.SQS_LIST_QUEUES_LIMIT;
import static com.amazonaws.services.sqs.util.SQSQueueUtils.forEachQueue;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.sqs.executors.SerializableReference;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

@SuppressWarnings("squid:S2055") // SonarLint exception for the custom serialization approach.
class IdleQueueSweeper extends SQSScheduledExecutorService implements Serializable {

    private static final long serialVersionUID = 27151245504960185L;

    private static final Log LOG = LogFactory.getLog(IdleQueueSweeper.class);
    
    private final SerializableReference<IdleQueueSweeper> thisReference;
    private final Consumer<Exception> exceptionHandler;

    public IdleQueueSweeper(AmazonSQSRequester sqsRequester, AmazonSQSResponder sqsResponder, String queueUrl, String queueNamePrefix,
            long period, TimeUnit unit, Consumer<Exception> exceptionHandler) {
        super(sqsRequester, sqsResponder, queueUrl, exceptionHandler);

        thisReference = new SerializableReference<>(queueUrl, this);
        this.exceptionHandler = exceptionHandler;

        // Jitter the startup times to avoid throttling on tagging as much as possible.
        long initialDelay = ThreadLocalRandom.current().nextLong(period);

        // TODO-RS: Invoke this repeatedly over time to ensure the task is reset
        // if it ever dies for any reason.
        repeatAtFixedRate(deduplicated(() -> checkQueuesForIdleness(queueNamePrefix)), 
                initialDelay, period, unit);
    }

    protected void checkQueuesForIdleness(String prefix) {
        LOG.info("Checking all queues begining with prefix " + prefix + " for idleness");

        try {
            forEachQueue(this, serializable(p -> sqs.listQueues(ListQueuesRequest.builder().queueNamePrefix(p).build()).queueUrls()), prefix,
                    SQS_LIST_QUEUES_LIMIT, (Serializable & Consumer<String>) this::checkQueueForIdleness);
        } catch (RejectedExecutionException e) {
            // Already shutting down, ignore
        } catch (Exception e) {
            // Make sure the recurring task never throws so it doesn't terminate.
            String message = "Encountered error when checking queues for idleness (prefix = " + prefix + ")";
            exceptionHandler.accept(new RuntimeException(message, e));
        }
    }

    protected void checkQueueForIdleness(String queueUrl) {
        try {
            if (isQueueIdle(queueUrl) && SQSQueueUtils.isQueueEmpty(sqs, queueUrl)) {
                LOG.info("Deleting idle queue: " + queueUrl);
                sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
            }
        } catch (QueueDoesNotExistException e) {
            // Queue already deleted so nothing to do.
        }
    }

    private boolean isQueueIdle(String queueUrl) {
        Map<String, String> queueTags = sqs.listQueueTags(ListQueueTagsRequest.builder().queueUrl(queueUrl).build()).tags();
        Long lastHeartbeat = AmazonSQSIdleQueueDeletingClient.getLongTag(queueTags, AmazonSQSIdleQueueDeletingClient.LAST_HEARTBEAT_TIMESTAMP_TAG);
        Long idleQueueRetentionPeriod = AmazonSQSIdleQueueDeletingClient.getLongTag(queueTags, AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD_TAG);
        long currentTimestamp = System.currentTimeMillis();

        return idleQueueRetentionPeriod != null && 
                (lastHeartbeat == null || 
                (currentTimestamp - lastHeartbeat) > idleQueueRetentionPeriod * 1000);
    }
    
    @Override
    protected SQSFutureTask<?> deserializeTask(Message message) {
        return thisReference.withScope(() -> super.deserializeTask(message));
    }
    
    protected Object writeReplace() throws ObjectStreamException {
        return thisReference.proxy();
    }
    
    @Override
    public void shutdown() {
        super.shutdown();
        thisReference.close();
    }
}
