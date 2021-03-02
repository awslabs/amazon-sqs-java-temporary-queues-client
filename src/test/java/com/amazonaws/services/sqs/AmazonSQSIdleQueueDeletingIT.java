package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.util.SQSMessageConsumerBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.util.IntegrationTest;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSQueueUtils;


public class AmazonSQSIdleQueueDeletingIT extends IntegrationTest {

    private static AmazonSQSIdleQueueDeletingClient client;
    private static String queueUrl;
    private static AmazonSQSRequester requester;
    private static AmazonSQSResponder responder;

    @Before
    public void setup() {
        client = new AmazonSQSIdleQueueDeletingClient(sqs, queueNamePrefix);
        requester = new AmazonSQSRequesterClient(sqs, queueNamePrefix,
                Collections.emptyMap(), Collections.emptyMap(), exceptionHandler);
        responder = new AmazonSQSResponderClient(sqs);
    }

    @After
    public void teardown() {
        if (client != null && queueUrl != null) {
            try {
                client.deleteQueue(queueUrl);
            } catch (QueueDoesNotExistException e) {
                // Ignore
            }
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
        client.startSweeper(requester, responder, 5, TimeUnit.SECONDS, exceptionHandler);
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "-IdleQueue")
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "1");
        queueUrl = client.createQueue(createQueueRequest).getQueueUrl();
        
        // May have to wait for up to a minute for the new queue to show up in ListQueues
        Assert.assertTrue("Expected queue to be deleted: " + queueUrl,
                          SQSQueueUtils.awaitQueueDeleted(sqs, queueUrl, 70, TimeUnit.SECONDS));
    }

    @Test
    public void updatedHeartBeatTag() throws InterruptedException {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "-HeartbeatTag")
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "60");
        queueUrl = client.createQueue(createQueueRequest).getQueueUrl();

        SendMessageRequest sendMsgRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody("hello world");
        client.sendMessage(sendMsgRequest);

        String initialHeartBeat = getLastHeartbeatTimestamp();

        // Wait 2 * heartbeatIntervalSeconds before sending message
        // so that heartbeatToQueueIfNecessary calls
        // heartbeatToQueue and update LAST_HEARTBEAT_TIMESTAMP_TAG
        TimeUnit.SECONDS.sleep(10);
        client.sendMessage(sendMsgRequest);

        String updatedHeartbeat = getLastHeartbeatTimestamp();

        Assert.assertNotEquals(initialHeartBeat, updatedHeartbeat);
    }

    private String getLastHeartbeatTimestamp() {
        return client
                .listQueueTags(queueUrl)
                .getTags()
                .get(AmazonSQSIdleQueueDeletingClient.LAST_HEARTBEAT_TIMESTAMP_TAG);
    }

    @Test
    public void notUpdatedHeartBeatTag() throws InterruptedException {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "-HeartbeatTag")
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "60");
        queueUrl = client.createQueue(createQueueRequest).getQueueUrl();

        SendMessageRequest sendMsgRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody("hello world");
        client.sendMessage(sendMsgRequest);


        String initialHeartBeat = getLastHeartbeatTimestamp();

        // Should skip call to heartbeatToQueue and not update LAST_HEARTBEAT_TIMESTAMP_TAG
        client.sendMessage(sendMsgRequest);

        String notUpdatedHeartbeat = getLastHeartbeatTimestamp();

        Assert.assertEquals(initialHeartBeat, notUpdatedHeartbeat);
    }
    
    @Test
    public void recreatingQueues() throws InterruptedException {
        String queueName = queueNamePrefix + "-DeletedTooSoon";
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueName)
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "60");
        queueUrl = client.createQueue(createQueueRequest).getQueueUrl();

        QueueUser user = new QueueUser();
        user.start();
     
        TimeUnit.SECONDS.sleep(5);
        
        // Use the underlying client so the wrapper has no chance to do anything first
        sqs.deleteQueue(queueUrl);
        
        // Sleeping is unfortunate here, but it's necessary to ensure the eventual consistency
        // of the delete is resolved first. Otherwise it's easy to get a false positive below.
        TimeUnit.MINUTES.sleep(1);
        
        // Ensure the original queue is eventually recreated. This becoming true at least once
        // indicates that CreateQueue was successfully called, even if it may flip back to false
        // on subsequent calls.
        Assert.assertTrue("Expected original queue to be recreated: " + queueUrl, 
                          SQSQueueUtils.awaitQueueCreated(sqs, queueUrl, 70, TimeUnit.SECONDS));
        
        // Ensure the user doesn't experience any send or receive failures
        user.finish();
        
        String failoverQueueName = AmazonSQSIdleQueueDeletingClient.alternateQueueName(queueName);
        String failoverQueueUrl = sqs.getQueueUrl(failoverQueueName).getQueueUrl();
        
        // Delete the queue through the client and ensure the failover queue is also deleted.
        // Eventual consistency is a problem here as well - the DeleteQueue may fail if
        // done too soon after recreating a queue.
        SQSQueueUtils.awaitWithPolling(2, 70, TimeUnit.SECONDS, () -> {
            try {
                client.deleteQueue(queueUrl);
                return true;
            } catch (QueueDoesNotExistException e) {
                return false;
            }
        });
        
        Assert.assertTrue("Expected original queue to be deleted: " + failoverQueueUrl, 
                SQSQueueUtils.awaitQueueDeleted(sqs, queueUrl, 70, TimeUnit.SECONDS));
        Assert.assertTrue("Expected failover queue to be deleted with the original: " + failoverQueueUrl, 
                SQSQueueUtils.awaitQueueDeleted(sqs, failoverQueueUrl, 70, TimeUnit.SECONDS));
    }
    
    private class QueueUser {
        
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        SQSMessageConsumer messageConsumer;
        
        public void start() {
            messageConsumer = SQSMessageConsumerBuilder.standard()
                                                       .withAmazonSQS(client)
                                                       .withQueueUrl(queueUrl)
                                                       .withConsumer(this::receiveMessage)
                                                       .withExceptionHandler(exceptionHandler)
                                                       .build();
            messageConsumer.start();
            executor.scheduleAtFixedRate(this::sendMessage, 0, 1, TimeUnit.SECONDS);
        }
        
        private void sendMessage() {
            try {
                client.sendMessage(queueUrl, "Message");
            } catch (RuntimeException e) {
                exceptionHandler.accept(e);
            }
        }
        
        private void receiveMessage(Message message) {
            // Ignore
        }
        
        public void finish() throws InterruptedException {
            executor.shutdown();
            messageConsumer.terminate();
        }
    }
}
