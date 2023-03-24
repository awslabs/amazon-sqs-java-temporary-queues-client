package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.util.Constants;
import com.amazonaws.services.sqs.util.IntegrationTest;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSMessageConsumerBuilder;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AmazonSQSIdleQueueDeletingIT extends IntegrationTest {

    private static AmazonSQSIdleQueueDeletingClient client;
    private static String queueUrl;
    private static AmazonSQSRequester requester;
    private static AmazonSQSResponder responder;

    @BeforeEach
    public void setup() {
        client = new AmazonSQSIdleQueueDeletingClient(sqs, queueNamePrefix);
        requester = new AmazonSQSRequesterClient(sqs, queueNamePrefix,
                Collections.emptyMap(), exceptionHandler);
        responder = new AmazonSQSResponderClient(sqs);
    }

    @AfterEach
    public void teardown() {
        if (client != null && queueUrl != null) {
            try {
                client.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
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
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put(Constants.IDLE_QUEUE_RETENTION_PERIOD, "1");
        client.startSweeper(requester, responder, 5, TimeUnit.SECONDS, exceptionHandler);
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "-IdleQueue")
                .attributesWithStrings(attributes).build();
        queueUrl = client.createQueue(createQueueRequest).queueUrl();
        
        // May have to wait for up to a minute for the new queue to show up in ListQueues
        assertTrue(SQSQueueUtils.awaitQueueDeleted(sqs, queueUrl, 70, TimeUnit.SECONDS),
                "Expected queue to be deleted: " + queueUrl);
    }

    @Test
    public void updatedHeartBeatTag() throws InterruptedException {
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put(Constants.IDLE_QUEUE_RETENTION_PERIOD, "60");
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "-HeartbeatTag")
                .attributesWithStrings(attributes).build();
        queueUrl = client.createQueue(createQueueRequest).queueUrl();

        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("hello world").build();
        client.sendMessage(sendMsgRequest);

        String initialHeartBeat = getLastHeartbeatTimestamp();

        // Wait 2 * heartbeatIntervalSeconds before sending message
        // so that heartbeatToQueueIfNecessary calls
        // heartbeatToQueue and update LAST_HEARTBEAT_TIMESTAMP_TAG
        TimeUnit.SECONDS.sleep(10);
        client.sendMessage(sendMsgRequest);

        String updatedHeartbeat = getLastHeartbeatTimestamp();

        assertNotEquals(initialHeartBeat, updatedHeartbeat);
    }

    private String getLastHeartbeatTimestamp() {
        return client
                .listQueueTags(ListQueueTagsRequest.builder().queueUrl(queueUrl).build())
                .tags()
                .get(AmazonSQSIdleQueueDeletingClient.LAST_HEARTBEAT_TIMESTAMP_TAG);
    }

    @Test
    public void notUpdatedHeartBeatTag() throws InterruptedException {
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put(Constants.IDLE_QUEUE_RETENTION_PERIOD, "60");
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "-HeartbeatTag")
                .attributesWithStrings(attributes).build();
        queueUrl = client.createQueue(createQueueRequest).queueUrl();

        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("hello world").build();
        client.sendMessage(sendMsgRequest);


        String initialHeartBeat = getLastHeartbeatTimestamp();

        // Should skip call to heartbeatToQueue and not update LAST_HEARTBEAT_TIMESTAMP_TAG
        client.sendMessage(sendMsgRequest);

        String notUpdatedHeartbeat = getLastHeartbeatTimestamp();

        assertEquals(initialHeartBeat, notUpdatedHeartbeat);
    }
    
    @Test
    public void recreatingQueues() throws InterruptedException {
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put(Constants.IDLE_QUEUE_RETENTION_PERIOD, "60");
        String queueName = queueNamePrefix + "-DeletedTooSoon";
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributesWithStrings(attributes).build();
        queueUrl = client.createQueue(createQueueRequest).queueUrl();

        QueueUser user = new QueueUser();
        user.start();
     
        TimeUnit.SECONDS.sleep(5);
        
        // Use the underlying client so the wrapper has no chance to do anything first
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        
        // Sleeping is unfortunate here, but it's necessary to ensure the eventual consistency
        // of the delete is resolved first. Otherwise, it's easy to get a false positive below.
        TimeUnit.MINUTES.sleep(1);
        
        // Ensure the original queue is eventually recreated. This becoming true at least once
        // indicates that CreateQueue was successfully called, even if it may flip back to false
        // on subsequent calls.
        assertTrue(SQSQueueUtils.awaitQueueCreated(sqs, queueUrl, 70, TimeUnit.SECONDS),
                "Expected original queue to be recreated: " + queueUrl);
        
        // Ensure the user doesn't experience any send or receive failures
        user.finish();
        
        String failoverQueueName = AmazonSQSIdleQueueDeletingClient.alternateQueueName(queueName);
        String failoverQueueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(failoverQueueName).build()).queueUrl();
        
        // Delete the queue through the client and ensure the failover queue is also deleted.
        // Eventual consistency is a problem here as well - the DeleteQueue may fail if
        // done too soon after recreating a queue.
        SQSQueueUtils.awaitWithPolling(2, 70, TimeUnit.SECONDS, () -> {
            try {
                client.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
                return true;
            } catch (QueueDoesNotExistException e) {
                return false;
            }
        });
        
        assertTrue(SQSQueueUtils.awaitQueueDeleted(sqs, queueUrl, 70, TimeUnit.SECONDS),
                "Expected original queue to be deleted: " + failoverQueueUrl);
        assertTrue(SQSQueueUtils.awaitQueueDeleted(sqs, failoverQueueUrl, 70, TimeUnit.SECONDS),
                "Expected failover queue to be deleted with the original: " + failoverQueueUrl);
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
                SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody("Message").build();
                client.sendMessage(sendMessageRequest);
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
