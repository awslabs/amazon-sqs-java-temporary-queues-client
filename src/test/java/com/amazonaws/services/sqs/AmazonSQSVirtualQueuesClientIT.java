package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.util.Constants;
import com.amazonaws.services.sqs.util.IntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AmazonSQSVirtualQueuesClientIT extends IntegrationTest {
    
    private static String hostQueueUrl;
    private static SqsClient client;

    BiConsumer<String, Message> orphanedMessageHandlerMock;

    @BeforeEach
    public void setup() {
        orphanedMessageHandlerMock = mock(BiConsumer.class);
        client = AmazonSQSVirtualQueuesClientBuilder.standard().withAmazonSQS(sqs).withOrphanedMessageHandler(orphanedMessageHandlerMock).build();
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "-HostQueue").build();
        hostQueueUrl = client.createQueue(createQueueRequest).queueUrl();
    }

    @AfterEach
    public void teardown() {
        if (hostQueueUrl != null) {
            client.deleteQueue(DeleteQueueRequest.builder().queueUrl(hostQueueUrl).build());
        }
        if (client != null) {
            client.close();
        }
    }
    
    @Test
    public void expiringVirtualQueue() throws InterruptedException {
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl);
        attributes.put(Constants.IDLE_QUEUE_RETENTION_PERIOD, "10");
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName("ShortLived")
                .attributesWithStrings(attributes).build();
        String virtualQueueUrl = client.createQueue(request).queueUrl();

        // Do a few long poll receives and validate the queue stays alive.
        // We expect empty receives but not errors.
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(virtualQueueUrl)
                .waitTimeSeconds(5).build();
        for (int i = 0; i < 3; i++) {
            assertEquals(0, client.receiveMessage(receiveRequest).messages().size());
        }
        
        // Now go idle for a while and the queue should be deleted.
        TimeUnit.SECONDS.sleep(12);

        assertThrows(QueueDoesNotExistException.class,
                () -> client.receiveMessage(ReceiveMessageRequest.builder().queueUrl(virtualQueueUrl).build()));
    }

    @Test
    public void ReceiveMessageWaitTimeSecondsNull() {
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl);
        attributes.put(Constants.IDLE_QUEUE_RETENTION_PERIOD, "5");
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName("ReceiveMessageWaitTimeSecondsNull")
                .attributesWithStrings(attributes).build();
        String virtualQueueUrl = client.createQueue(request).queueUrl();

        // Do Receive message request with null WaitTimeSeconds.
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(virtualQueueUrl).build();

        assertThrows(NullPointerException.class,
                () -> assertEquals(0, client.receiveMessage(receiveRequest).messages().size()));

        // Delete the queue, so we don't get a spurious message about it expiring during the test shutdown
        client.deleteQueue(DeleteQueueRequest.builder().queueUrl(virtualQueueUrl).build());
    }

    @Test
    public void virtualQueueShouldNotExpireDuringLongReceive() throws InterruptedException {
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl);
        attributes.put(Constants.IDLE_QUEUE_RETENTION_PERIOD, "10");
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName("ShortLived")
                .attributesWithStrings(attributes).build();
        String virtualQueueUrl = client.createQueue(request).queueUrl();

        // Do a single long receive call, longer than the retention period.
        // This tests that the queue is still considered in use during the call
        // and not just at the beginning.
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(virtualQueueUrl)
                .waitTimeSeconds(20).build();
        assertEquals(0, client.receiveMessage(receiveRequest).messages().size());

        // Ensure the queue still exists
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(virtualQueueUrl).messageBody("Boy I'm sure glad you didn't get deleted").build();
        client.sendMessage(sendMessageRequest);
        assertEquals(1, client.receiveMessage(receiveRequest).messages().size());

        // Delete the queue, so we don't get a spurious message about it expiring during the test shutdown
        client.deleteQueue(DeleteQueueRequest.builder().queueUrl(virtualQueueUrl).build());
    }

    @Test
    public void missingMessageAttributeIsReceivedAndDeleted() throws InterruptedException {
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl);
        attributes.put(Constants.IDLE_QUEUE_RETENTION_PERIOD, "10");
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName("ShortLived")
                .attributesWithStrings(attributes).build();
        String virtualQueueUrl = client.createQueue(request).queueUrl();

        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(virtualQueueUrl)
                .waitTimeSeconds(20).build();
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(hostQueueUrl)
                .messageBody("Missing Message attributes!")
                .delaySeconds(5).build();

        client.sendMessage(sendMessageRequest);
        // Message sent with missing attribute is deleted
        assertEquals(0, client.receiveMessage(receiveRequest).messages().size());
        verify(orphanedMessageHandlerMock).accept(any(), any());
    }
}
