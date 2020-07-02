package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD;
import static com.amazonaws.services.sqs.AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.Map;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.sqs.util.IntegrationTest;
import org.junit.jupiter.api.Assertions;

public class AmazonSQSTemporaryQueuesClientIT extends IntegrationTest {

    private AmazonSQSTemporaryQueuesClient client;
    private String queueUrl;

    @After
    public void teardown() {
        if (queueUrl != null) {
            client.deleteQueue(queueUrl);
        }
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    public void createQueueAddsAttributes() {
        createQueue(null);
    }

    @Test
    public void createQueueWithUnsupportedAttributes() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            setupClient(null);
            client.createQueue(new CreateQueueRequest()
                    .withQueueName(queueNamePrefix + "InvalidQueue")
                    .withAttributes(Collections.singletonMap(QueueAttributeName.FifoQueue.name(), "true")));
        });
    }

    @Test
    public void createQueueWithUpdatedMaxIdleQueueRetentionPeriod() {
        createQueue(600L);
    }

    @Test
    public void createQueueWithUnsupportedMaxIdleQueueRetentionPeriod() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            setupClient(1L);
        });
    }

    private void setupClient(Long maxIdleQueueRetentionPeriod) {
        AmazonSQSRequesterClientBuilder requesterBuilder;

        if (maxIdleQueueRetentionPeriod != null) {
            AmazonSQSIdleQueueDeletingClient.setMaxIdleQueueRetentionPeriod(maxIdleQueueRetentionPeriod);
        }

        requesterBuilder =
                AmazonSQSRequesterClientBuilder.standard()
                        .withAmazonSQS(sqs)
                        .withInternalQueuePrefix(queueNamePrefix);

        client = AmazonSQSTemporaryQueuesClient.make(requesterBuilder);
    }

    private void createQueue(Long maxIdleQueueRetentionPeriod) {
        setupClient(maxIdleQueueRetentionPeriod);
        queueUrl = client.createQueue(queueNamePrefix + "TestQueue").getQueueUrl();
        Map<String, String> attributes = client.getQueueAttributes(queueUrl, Collections.singletonList("All")).getAttributes();
        String hostQueueUrl = attributes.get(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);
        assertNotNull(hostQueueUrl);
        Assert.assertEquals("300", attributes.get(IDLE_QUEUE_RETENTION_PERIOD));

        Map<String, String> hostQueueAttributes = client.getQueueAttributes(queueUrl, Collections.singletonList("All")).getAttributes();
        Assert.assertEquals("300", hostQueueAttributes.get(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD));
    }
}
