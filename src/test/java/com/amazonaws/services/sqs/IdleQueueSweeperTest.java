package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import com.amazonaws.services.sqs.util.TestUtils;

public class IdleQueueSweeperTest extends TestUtils {

    private static final String PREFIX = IdleQueueSweeper.class.getSimpleName() + "Test";
    
    private static AmazonSQS sqs;
    private static AmazonSQSResponsesClient sqsWithResponses;
    private static String sweepingQueueUrl;
    private static IdleQueueSweeper sweeper;

    @Before
    public void setup() {
        sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
        sqsWithResponses = new AmazonSQSResponsesClient(sqs);
        
        String sweepingQueueName = generateRandomQueueName(PREFIX);
        sweepingQueueUrl = sqs.createQueue(sweepingQueueName).getQueueUrl();
        sweeper = new IdleQueueSweeper(sqsWithResponses, sqsWithResponses, sweepingQueueUrl, PREFIX, 5, TimeUnit.SECONDS);
    }
    
    @After
    public void teardown() {
        if (sweeper != null) {
            sweeper.shutdown();
        }
        if (sweepingQueueUrl != null) {
            sqs.deleteQueue(sweepingQueueUrl);
        }
        if (sqsWithResponses != null) {
            sqsWithResponses.shutdown();
        }
        if (sqs != null) {
            sqs.shutdown();
        }
    }
    
    @Test
    public void idleQueueSweeper() throws InterruptedException {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(generateRandomQueueName(PREFIX + "_TestQueue_"));
        String idleQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        sqs.tagQueue(idleQueueUrl, Collections.singletonMap(
                AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "1"));
        
        // May have to wait for up to a minute for the new queue to show up in ListQueues
        boolean deleted = SQSQueueUtils.awaitWithPolling(2, 70, TimeUnit.SECONDS, () -> {
            try {
                sqs.listQueueTags(idleQueueUrl);
                return false;
            } catch (QueueDoesNotExistException e) {
                return true;
            }
        });
        Assert.assertTrue("Expected queue to be deleted: " + idleQueueUrl, deleted);
    }
}
