package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD;
import static com.amazonaws.services.sqs.AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.util.IntegrationTest;
import org.junit.jupiter.api.Assertions;

public class AmazonSQSTemporaryQueuesClientIT extends IntegrationTest {

    private AmazonSQSTemporaryQueuesClient client;
    private String queueUrl;
    
    @Before
    public void setup() {
        AmazonSQSRequesterClientBuilder requesterBuilder =
                AmazonSQSRequesterClientBuilder.standard()
                    .withAmazonSQS(sqs)
                    .withInternalQueuePrefix(queueNamePrefix);
        client = AmazonSQSTemporaryQueuesClient.make(requesterBuilder);
    }
    
    @After
    public void teardown() {
        if (queueUrl != null) {
            client.deleteQueue(queueUrl);
        }
        client.shutdown();
    }
    
    @Test
    public void createQueueAddsAttributes() {
        queueUrl = client.createQueue(queueNamePrefix + "TestQueue").getQueueUrl();
        Map<String, String> attributes = client.getQueueAttributes(queueUrl, Collections.singletonList("All")).getAttributes();
        String hostQueueUrl = attributes.get(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);
        assertNotNull(hostQueueUrl);
        Assert.assertEquals("300", attributes.get(IDLE_QUEUE_RETENTION_PERIOD));
        
        Map<String, String> hostQueueAttributes = client.getQueueAttributes(queueUrl, Collections.singletonList("All")).getAttributes();
        Assert.assertEquals("300", hostQueueAttributes.get(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD));
    }

    @Test
    public void createQueueWithUnsupportedAttributes() {
        try {
            client.createQueue(new CreateQueueRequest()
                    .withQueueName(queueNamePrefix + "InvalidQueue")
                    .withAttributes(Collections.singletonMap(QueueAttributeName.FifoQueue.name(), "true")));
            Assert.fail("Shouldn't be able to create a FIFO temporary queue");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Cannot create a temporary queue with the following attributes: FifoQueue", e.getMessage());
        }
    }

    @Test
    public void createQueueConfigurableIdleQueueRetentionPeriod() {
        AmazonSQSRequesterClientBuilder requesterBuilder =
                AmazonSQSRequesterClientBuilder.standard()
                        .withAmazonSQS(sqs)
                        .withInternalQueuePrefix(queueNamePrefix)
                        .withQueueRetentionPeriodSeconds(200);
        client = AmazonSQSTemporaryQueuesClient.make(requesterBuilder);

        queueUrl = client.createQueue(queueNamePrefix + "TestQueue").getQueueUrl();
        Map<String, String> attributes = client.getQueueAttributes(queueUrl, Collections.singletonList("All")).getAttributes();
        String hostQueueUrl = attributes.get(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);
        assertNotNull(hostQueueUrl);
        Assert.assertEquals("200", attributes.get(IDLE_QUEUE_RETENTION_PERIOD));

        Map<String, String> hostQueueAttributes = client.getQueueAttributes(queueUrl, Collections.singletonList("All")).getAttributes();
        Assert.assertEquals("200", hostQueueAttributes.get(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD));
    }

    @Test
    public void createQueueWithUnsupportedIdleQueueRetentionPeriod() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            AmazonSQSRequesterClientBuilder requesterBuilder =
                    AmazonSQSRequesterClientBuilder.standard()
                            .withAmazonSQS(sqs)
                            .withInternalQueuePrefix(queueNamePrefix)
                            .withQueueRetentionPeriodSeconds(500);
            client = AmazonSQSTemporaryQueuesClient.make(requesterBuilder);
        });
    }
}
