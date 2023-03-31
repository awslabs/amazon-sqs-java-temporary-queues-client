package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.util.Constants;
import com.amazonaws.services.sqs.util.IntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AmazonSQSVirtualQueuesClientIT extends IntegrationTest {
    
    private static String hostQueueUrl;
    private static AmazonSQS client;

    BiConsumer<String, Message> orphanedMessageHandlerMock;

    @BeforeEach
    public void setup() {
        orphanedMessageHandlerMock = mock(BiConsumer.class);
        client = AmazonSQSVirtualQueuesClientBuilder.standard().withAmazonSQS(sqs).withOrphanedMessageHandler(orphanedMessageHandlerMock).build();
        hostQueueUrl = client.createQueue(queueNamePrefix + "-HostQueue").getQueueUrl();
    }

    @AfterEach
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
                .addAttributesEntry(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl)
                .addAttributesEntry(Constants.IDLE_QUEUE_RETENTION_PERIOD, "10");
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

        assertThrows(QueueDoesNotExistException.class, () -> client.receiveMessage(virtualQueueUrl));
    }

    @Test
    public void ReceiveMessageWaitTimeSecondsNull() {
        CreateQueueRequest request = new CreateQueueRequest()
                .withQueueName("ReceiveMessageWaitTimeSecondsNull")
                .addAttributesEntry(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl)
                .addAttributesEntry(Constants.IDLE_QUEUE_RETENTION_PERIOD, "5");
        String virtualQueueUrl = client.createQueue(request).getQueueUrl();

        // Do Receive message request with null WaitTimeSeconds.
        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(virtualQueueUrl);

        assertThrows(NullPointerException.class,
                () -> assertEquals(0, client.receiveMessage(receiveRequest).getMessages().size()));

        // Delete the queue so we don't get a spurious message about it expiring during the test shutdown
        client.deleteQueue(virtualQueueUrl);
    }

    @Test
    public void virtualQueueShouldNotExpireDuringLongReceive() throws InterruptedException {
        CreateQueueRequest request = new CreateQueueRequest()
                .withQueueName("ShortLived")
                .addAttributesEntry(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl)
                .addAttributesEntry(Constants.IDLE_QUEUE_RETENTION_PERIOD, "10");
        String virtualQueueUrl = client.createQueue(request).getQueueUrl();

        // Do a single long receive call, longer than the retention period.
        // This tests that the queue is still considered in use during the call
        // and not just at the beginning.
        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(virtualQueueUrl)
                .withWaitTimeSeconds(20);
        assertEquals(0, client.receiveMessage(receiveRequest).getMessages().size());

        // Ensure the queue still exists
        client.sendMessage(virtualQueueUrl, "Boy I'm sure glad you didn't get deleted");
        assertEquals(1, client.receiveMessage(receiveRequest).getMessages().size());

        // Delete the queue so we don't get a spurious message about it expiring during the test shutdown
        client.deleteQueue(virtualQueueUrl);
    }

    @Test
    public void missingMessageAttributeIsReceivedAndDeleted() throws InterruptedException {
        CreateQueueRequest request = new CreateQueueRequest()
                .withQueueName("ShortLived")
                .addAttributesEntry(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueueUrl)
                .addAttributesEntry(Constants.IDLE_QUEUE_RETENTION_PERIOD, "10");
        String virtualQueueUrl = client.createQueue(request).getQueueUrl();

        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(virtualQueueUrl)
                .withWaitTimeSeconds(20);
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(hostQueueUrl)
                .withMessageBody("Missing Message attributes!")
                .withDelaySeconds(5);

        client.sendMessage(sendMessageRequest);
        // Message sent with missing attribute is deleted
        assertEquals(0, client.receiveMessage(receiveRequest).getMessages().size());
        verify(orphanedMessageHandlerMock).accept(any(), any());
    }
}
