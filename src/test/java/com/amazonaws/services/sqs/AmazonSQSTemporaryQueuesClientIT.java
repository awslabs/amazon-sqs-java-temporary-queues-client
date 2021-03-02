package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD;
import static com.amazonaws.services.sqs.AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
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
        createQueueShouldSetRetentionPeriod(null);
    }

    @Test
    public void createQueueAddsTags() {
        Map<String, String> tags = new HashMap<>();
        tags.put("Tag1", "Value1");
        tags.put("Tag2", "Value2");
        createQueueShouldSetTags(tags);
    }

    @Test
    public void createQueueWithUnsupportedAttributes() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            setupClient(null, new HashMap<>());
            client.createQueue(new CreateQueueRequest()
                    .withQueueName(queueNamePrefix + "InvalidQueue")
                    .withAttributes(Collections.singletonMap(QueueAttributeName.FifoQueue.name(), "true")));
        });
    }

    @Test
    public void createQueueConfigurableIdleQueueRetentionPeriod() {
        createQueueShouldSetRetentionPeriod(200L);
    }

    @Test
    public void createQueueWithUnsupportedIdleQueueRetentionPeriod() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            setupClient(-10L, new HashMap<>());
        });
    }

    private void setupClient(Long idleQueueRetentionPeriod, Map<String, String> tags) {
        AmazonSQSRequesterClientBuilder requesterBuilder;

        requesterBuilder =
                AmazonSQSRequesterClientBuilder.standard()
                        .withAmazonSQS(sqs)
                        .withInternalQueuePrefix(queueNamePrefix)
                        .withQueueTags(tags);
        if (idleQueueRetentionPeriod != null) {
            requesterBuilder = requesterBuilder.withIdleQueueRetentionPeriodSeconds(idleQueueRetentionPeriod);
        }

        client = AmazonSQSTemporaryQueuesClient.make(requesterBuilder);
    }

    private void createQueueShouldSetRetentionPeriod(Long idleQueueRetentionPeriod) {
        setupClient(idleQueueRetentionPeriod, new HashMap<>());
        idleQueueRetentionPeriod = (idleQueueRetentionPeriod != null) ? idleQueueRetentionPeriod : 300L;
        queueUrl = client.createQueue(queueNamePrefix + "TestQueue").getQueueUrl();
        Map<String, String> attributes = client.getQueueAttributes(queueUrl, Collections.singletonList("All")).getAttributes();
        String hostQueueUrl = attributes.get(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);
        assertNotNull(hostQueueUrl);
        Assert.assertEquals(Long.toString(idleQueueRetentionPeriod), attributes.get(IDLE_QUEUE_RETENTION_PERIOD));

        Map<String, String> hostQueueAttributes = client.getQueueAttributes(queueUrl, Collections.singletonList("All")).getAttributes();
        Assert.assertEquals(Long.toString(idleQueueRetentionPeriod), hostQueueAttributes.get(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD));
    }

    private void createQueueShouldSetTags(Map<String, String> tags) {
        setupClient(null, tags);
        CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(queueNamePrefix + "TestQueue");
        queueUrl = client.createQueue(createQueueRequest).getQueueUrl();
        Map<String, String> actualTags = client.listQueueTags(queueUrl).getTags();
        tags.forEach((k, v) -> {
            assertTrue(actualTags.containsKey(k));
            assertEquals(v, actualTags.get(k));
        });
    }
}
