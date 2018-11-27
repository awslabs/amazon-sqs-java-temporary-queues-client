package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.util.TestUtils;

public class AmazonSQSVirtualQueuesClientTest extends TestUtils {
    
    private static AmazonSQS sqs;
    private static String hostQueueUrl;
    private static AmazonSQSVirtualQueuesClient client;

    @Before
    public void setup() {
        sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
        client = new AmazonSQSVirtualQueuesClient(sqs);
        hostQueueUrl = client.createQueue(generateRandomQueueName()).getQueueUrl();
    }

    @After
    public void teardown() {
        if (hostQueueUrl != null) {
            client.deleteQueue(hostQueueUrl);
        }
        if (client != null) {
            client.shutdown();
        }
        if (sqs != null) {
            sqs.shutdown();
        }
    }
    
    @Test
    public void expiringVirtualQueue() throws InterruptedException {
        CreateQueueRequest request = new CreateQueueRequest()
                .withQueueName("ShortLived")
                .addAttributesEntry(AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl)
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "10");
        String virtualQueueUrl = client.createQueue(request).getQueueUrl();
        
        // Do a few long poll receives and validate the queue stays alive.
        // We expect empty receives but not errors.
        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(virtualQueueUrl)
                .withWaitTimeSeconds(5);
        for (int i = 0; i < 3; i++) {
            assertEquals(0, client.receiveMessage(receiveRequest).getMessages().size());
        }
        
        // Now go idle for a while and the queue should be deleted.
        TimeUnit.SECONDS.sleep(12);
        
        try {
            client.receiveMessage(virtualQueueUrl);
            fail("Expected queue to be automatically deleted: " + virtualQueueUrl);
        } catch (QueueDoesNotExistException e) {
            // Expected
        }
    }
}
