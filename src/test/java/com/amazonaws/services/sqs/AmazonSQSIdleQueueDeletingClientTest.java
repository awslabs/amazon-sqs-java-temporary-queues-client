package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import com.amazonaws.services.sqs.util.TestUtils;

public class AmazonSQSIdleQueueDeletingClientTest extends TestUtils {

    private static String prefix;

    private static AmazonSQS sqs;
    private static AmazonSQSIdleQueueDeletingClient client;

    @Before
    public void setup() {
        // UUIDs are too long for this
        prefix = "IdleQueueDeletingClientTest" + ThreadLocalRandom.current().nextInt(1000000);
        
        sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_WEST_2).build();

        client = new AmazonSQSIdleQueueDeletingClient(sqs, prefix);
    }

    @After
    public void teardown() {
        if (client != null) {
            client.teardown();
        }
        if (sqs != null) {
            sqs.shutdown();
        }
    }

    @Test
    public void idleQueueIsDeleted() throws InterruptedException {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(generateRandomQueueName(prefix))
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "1");
        String idleQueueUrl = client.createQueue(createQueueRequest).getQueueUrl();
        
        try {
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
            try {
                client.deleteQueue(idleQueueUrl);
            } catch (QueueDoesNotExistException e) {
                // Expected
            }
        }
    }
    
    @Test
    public void recreatingQueues() throws InterruptedException {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(generateRandomQueueName(prefix))
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "60");
        String queueUrl = client.createQueue(createQueueRequest).getQueueUrl();

        try {
            // Use the underlying client so the wrapper has no chance to do anything first
            sqs.deleteQueue(queueUrl);
            
            // TODO-RS: This should be continuously using the queue during both
            // failover and recovery
            TimeUnit.MINUTES.sleep(1);
            
            String messageBody = "Whatever, I'm still sending a message!";
            client.sendMessage(queueUrl, messageBody);
            
            ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withWaitTimeSeconds(20);
            List<Message> received = client.receiveMessage(receiveRequest).getMessages();
            assertEquals(1, received.size());
            assertEquals(messageBody, received.get(0).getBody());
        } finally {
            try {
                client.deleteQueue(queueUrl);
            } catch (QueueDoesNotExistException e) {
                // Expected
            }
        }
    }
}
