package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.executors.DeduplicatedRunnable.deduplicated;
import static com.amazonaws.services.sqs.util.SQSQueueUtils.SQS_LIST_QUEUES_LIMIT;
import static com.amazonaws.services.sqs.util.SQSQueueUtils.forEachQueue;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.sqs.executors.SQSScheduledExecutorService;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;
import com.amazonaws.services.sqs.util.SQSQueueUtils;

class IdleQueueSweeper extends SQSScheduledExecutorService {
	
    private static final Log LOG = LogFactory.getLog(AmazonSQSIdleQueueDeletingClient.class);

    public IdleQueueSweeper(AmazonSQSWithResponses sqs, String queueUrl, String queueNamePrefix, long period, TimeUnit unit) {
		super(sqs, queueUrl, null);
		
		// Jitter the startup times to avoid throttling on tagging as much as possible.
		long initialDelay = ThreadLocalRandom.current().nextLong(period);
		
		// TODO-RS: Invoke this repeatedly over time to ensure the task is reset
		// if it ever dies for any reason.
        repeatAtFixedRate(deduplicated(() -> checkQueuesForIdleness(queueNamePrefix)), 
                          initialDelay, period, unit);
	}

	protected void checkQueuesForIdleness(String prefix) {
		try {
			forEachQueue(AmazonSQSIdleQueueDeletingClient.executor, sqs, prefix, SQS_LIST_QUEUES_LIMIT, this::checkQueueForIdleness);
		} catch (Exception e) {
			// Make sure the recurring task never throws so it doesn't terminate.
			LOG.error("Encounted error when checking queues for idleness (prefix = " + prefix + ")", e);
		}
    }
    
    protected void checkQueueForIdleness(String queueUrl) {
        try {
            if (isQueueIdle(queueUrl) && SQSQueueUtils.isQueueEmpty(sqs, queueUrl)) {
                sqs.deleteQueue(queueUrl);
            }
        } catch (QueueDoesNotExistException e) {
            // Queue already deleted so nothing to do.
        }
    }
    
    private boolean isQueueIdle(String queueUrl) {
        Map<String, String> queueTags = sqs.listQueueTags(queueUrl).getTags();
        Long lastHeartbeat = AmazonSQSIdleQueueDeletingClient.getLongTag(queueTags, AmazonSQSIdleQueueDeletingClient.LAST_HEARTBEAT_TIMESTAMP);
        Long idleQueueRetentionPeriod = AmazonSQSIdleQueueDeletingClient.getLongTag(queueTags, AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD);
        long currentTimestamp = System.currentTimeMillis();

        return idleQueueRetentionPeriod != null && 
        	   (lastHeartbeat == null || 
        		(currentTimestamp - lastHeartbeat) > idleQueueRetentionPeriod * 1000);
    }
}