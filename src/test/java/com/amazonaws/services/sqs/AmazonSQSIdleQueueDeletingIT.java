package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.util.IntegrationTest;
import com.amazonaws.services.sqs.util.SQSQueueUtils;

public class AmazonSQSIdleQueueDeletingIT extends IntegrationTest {

    private static AmazonSQSIdleQueueDeletingClient client;
    private static String queueUrl;
    private static AmazonSQSRequester requester;
    private static AmazonSQSResponder responder;

    @Before
    public void setup() {
        client = new AmazonSQSIdleQueueDeletingClient(sqs, queueNamePrefix);
        requester = new AmazonSQSRequesterClient(sqs, queueNamePrefix);
        responder = new AmazonSQSResponderClient(sqs);
    }

    @After
    public void teardown() {
        if (client != null && queueUrl != null) {
            client.deleteQueue(queueUrl);
        }
        if (responder != null) {
            responder.shutdown();
        }
        if (requester != null) {
            requester.shutdown();
        }
        if (client != null) {
            client.teardown();
        }
    }

    @Test
    public void idleQueueIsDeleted() throws InterruptedException {
        client.startSweeper(requester, responder, exceptionHandler);
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "-IdleQueue")
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "1");
        queueUrl = client.createQueue(createQueueRequest).getQueueUrl();
        
        // May have to wait for up to a minute for the new queue to show up in ListQueues
        boolean deleted = SQSQueueUtils.awaitWithPolling(2, 70, TimeUnit.SECONDS, () -> {
            try {
                sqs.listQueueTags(queueUrl);
                return false;
            } catch (QueueDoesNotExistException e) {
                return true;
            }
        });
        Assert.assertTrue("Expected queue to be deleted: " + queueUrl, deleted);
    }
    
    @Test
    public void recreatingQueues() throws InterruptedException {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "-DeletedTooSoon")
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "60");
        String queueUrl = client.createQueue(createQueueRequest).getQueueUrl();

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
    }
}
