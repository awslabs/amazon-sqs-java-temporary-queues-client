package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.SendMessageResult;
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
        requester = new AmazonSQSRequesterClient(sqs, queueNamePrefix);
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
        client.startSweeper(requester, responder, exceptionHandler);
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "-IdleQueue")
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "1");
        queueUrl = client.createQueue(createQueueRequest).getQueueUrl();
        
        // May have to wait for up to a minute for the new queue to show up in ListQueues
        Assert.assertTrue("Expected queue to be deleted: " + queueUrl, SQSQueueUtils.awaitQueueDeleted(sqs, queueUrl, 70, TimeUnit.SECONDS));
    }
    
    @Test
    public void recreatingQueues() throws InterruptedException {
        String queueName = queueNamePrefix + "-DeletedTooSoon";
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueName)
                .addAttributesEntry(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD, "60");
        queueUrl = client.createQueue(createQueueRequest).getQueueUrl();

        // Use the underlying client so the wrapper has no chance to do anything first
        sqs.deleteQueue(queueUrl);
        
        // Sleeping is unfortunate here, but it's necessary to ensure we don't lose any
        // sent messages due to eventual consistency.
        TimeUnit.MINUTES.sleep(1);
        
        QueueUser user = new QueueUser();
        user.start();
        
        // Ensure the original queue is eventually recreated. This becoming true at least once
        // indicates that CreateQueue was successfully called, even if it may flip back to false
        // on subsequent calls.
        Assert.assertTrue("Expected original queue to be recreated: " + queueUrl, 
                          SQSQueueUtils.awaitQueueCreated(sqs, queueUrl, 70, TimeUnit.SECONDS));
        
        // Ensure the user doesn't experience any send failures or message loss
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
        
        private final Set<String> sentMessageIDs = Collections.synchronizedSet(new HashSet<>());
        private final Set<String> receivedMessageIDs = Collections.synchronizedSet(new HashSet<>());
        
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        SQSMessageConsumer messageConsumer;
        
        public void start() {
            messageConsumer = new SQSMessageConsumer(client, queueUrl, this::receiveMessage);
            messageConsumer.start();
            executor.scheduleAtFixedRate(this::sendMessage, 0, 1, TimeUnit.SECONDS);
        }
        
        private void sendMessage() {
            SendMessageResult result = client.sendMessage(queueUrl, "Message");
            System.out.println("Sent: " + result.getMessageId());
            sentMessageIDs.add(result.getMessageId());
        }
        
        private void receiveMessage(Message message) {
            System.out.println("Received: " + message.getMessageId());
            receivedMessageIDs.add(message.getMessageId());
        }
        
        public void finish() throws InterruptedException {
            // Stop the sending first, then wait a bit to giving the consumer a change to
            // receive the rest of the messages.
            executor.shutdown();
            
            SQSQueueUtils.awaitWithPolling(1, 20, TimeUnit.SECONDS, () -> stuckMessages().isEmpty());
            Set<String> missingMessages = stuckMessages();
            Assert.assertTrue("Never received these messages: " + missingMessages, missingMessages.isEmpty());
            
            messageConsumer.shutdown();
        }
        
        public Set<String> stuckMessages() {
            Set<String> missingMessages = new HashSet<>(sentMessageIDs);
            missingMessages.removeAll(receivedMessageIDs);
            return missingMessages;
        }
    }
}
