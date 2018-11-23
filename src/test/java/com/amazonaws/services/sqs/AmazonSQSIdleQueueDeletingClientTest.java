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
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import com.amazonaws.services.sqs.util.TestUtils;

public class AmazonSQSIdleQueueDeletingClientTest extends TestUtils {

    private static AmazonSQS sqs;
    private static AmazonSQSWithResponses sqsWithResponses;

    @Before
    public void setup() {
        sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
        sqsWithResponses = new AmazonSQSResponsesClient(sqs);
    }
    
    @After
    public void teardown() {
        if (sqsWithResponses != null) {
            sqsWithResponses.shutdown();
        }
        if (sqs != null) {
            sqs.shutdown();
        }
    }
    
    @Test
    public void idleQueueSweeper() throws InterruptedException {
        String prefix = IdleQueueSweeper.class.getSimpleName() + "Test";
        String sweepingQueueName = generateRandomQueueName(prefix);
        String sweepingQueueUrl = sqs.createQueue(sweepingQueueName).getQueueUrl();
        IdleQueueSweeper sweeper = new IdleQueueSweeper(sqsWithResponses, sweepingQueueUrl, prefix, 5, TimeUnit.SECONDS);
        try {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                    .withQueueName(generateRandomQueueName(prefix + "_TestQueue_"));
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
        } finally {
            sqs.deleteQueue(sweepingQueueUrl);
            sweeper.shutdown();
        }
    }
    
    @Test
    public void idleQueueIsDeleted() throws InterruptedException {
        String prefix = IdleQueueSweeper.class.getSimpleName() + "Test";
        String sweepingQueueName = generateRandomQueueName(prefix);
        String sweepingQueueUrl = sqs.createQueue(sweepingQueueName).getQueueUrl();
        AmazonSQSIdleQueueDeletingClient client = new AmazonSQSIdleQueueDeletingClient(sqsWithResponses, prefix, sweepingQueueUrl);
        
        try {
            CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                    .withQueueName(generateRandomQueueName(prefix + "_TestQueue_"))
                    .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "1");
            String idleQueueUrl = client.createQueue(createQueueRequest).getQueueUrl();
            
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
        } finally {
            sqs.deleteQueue(sweepingQueueUrl);
            client.shutdown();
        }
    }
}
