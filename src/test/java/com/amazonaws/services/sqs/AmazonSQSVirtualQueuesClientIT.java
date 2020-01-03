package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.util.IntegrationTest;

public class AmazonSQSVirtualQueuesClientIT extends IntegrationTest {
    
    private static String hostQueueUrl;
    private static AmazonSQS client;

    @Before
    public void setup() {
        client = AmazonSQSVirtualQueuesClientBuilder.standard().withAmazonSQS(sqs).build();
        hostQueueUrl = client.createQueue(queueNamePrefix + "-HostQueue").getQueueUrl();
    }

    @After
    public void teardown() {
        if (hostQueueUrl != null) {
            client.deleteQueue(hostQueueUrl);
        }
        if (client != null) {
            client.shutdown();
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

    @Test
    public void ReceiveMessageWaitTimeSecondsNull() {
        CreateQueueRequest request = new CreateQueueRequest()
                .withQueueName("ReceiveMessageWaitTimeSecondsNull")
                .addAttributesEntry(AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl)
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "5");
        String virtualQueueUrl = client.createQueue(request).getQueueUrl();

        // Do Receive message request with null WaitTimeSeconds.
        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(virtualQueueUrl);
        try {
            assertEquals(0, client.receiveMessage(receiveRequest).getMessages().size());
        } catch (NullPointerException npe) {
            fail("NPE not expected with null WaitTimeSeconds on ReceiveMessageRequest");
        }
    }
}
