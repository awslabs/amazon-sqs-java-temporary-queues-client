package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.util.Constants.IDLE_QUEUE_RETENTION_PERIOD;
import static com.amazonaws.services.sqs.util.Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.Map;

import com.amazonaws.services.sqs.util.Constants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.sqs.util.IntegrationTest;
import org.junit.jupiter.api.Assertions;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

public class AmazonSQSTemporaryQueuesClientIT extends IntegrationTest {

    private AmazonSQSTemporaryQueuesClient client;
    private String queueUrl;
    
    @After
    public void teardown() {
        if (queueUrl != null) {
            client.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        }
        if (client != null) {
            client.close();
        }
    }
    
    @Test
    public void createQueueAddsAttributes() {
        createQueueShouldSetRetentionPeriod(null);
    }

    @Test
    public void createQueueWithUnsupportedAttributes() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            setupClient(null);
            client.createQueue(CreateQueueRequest.builder()
                    .queueName(queueNamePrefix + "InvalidQueue")
                    .attributesWithStrings(Collections.singletonMap(QueueAttributeName.FIFO_QUEUE.toString(), "true")).build());
        });
    }

    @Test
    public void createQueueConfigurableIdleQueueRetentionPeriod() {
        createQueueShouldSetRetentionPeriod(200L);
    }

    @Test
    public void createQueueWithUnsupportedIdleQueueRetentionPeriod() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            setupClient(-10L);
        });
    }

    private void setupClient(Long idleQueueRetentionPeriod) {
        AmazonSQSRequesterClientBuilder requesterBuilder;

        requesterBuilder =
                AmazonSQSRequesterClientBuilder.standard()
                        .withAmazonSQS(sqs)
                        .withInternalQueuePrefix(queueNamePrefix);
        if (idleQueueRetentionPeriod != null) {
            requesterBuilder = requesterBuilder.withIdleQueueRetentionPeriodSeconds(idleQueueRetentionPeriod);
        }

        client = AmazonSQSTemporaryQueuesClient.make(requesterBuilder);
    }

    private void createQueueShouldSetRetentionPeriod(Long idleQueueRetentionPeriod) {
        setupClient(idleQueueRetentionPeriod);
        idleQueueRetentionPeriod = (idleQueueRetentionPeriod != null) ? idleQueueRetentionPeriod : 300L;
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "TestQueue").build();
        queueUrl = client.createQueue(createQueueRequest).queueUrl();
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl).attributeNamesWithStrings(Collections.singletonList("All")).build();
        Map<String, String> attributes = client.getQueueAttributes(getQueueAttributesRequest).attributesAsStrings();
        String hostQueueUrl = attributes.get(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);
        assertNotNull(hostQueueUrl);
        Assert.assertEquals(Long.toString(idleQueueRetentionPeriod), attributes.get(IDLE_QUEUE_RETENTION_PERIOD));

        GetQueueAttributesRequest hostGetQueueAttributes = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl).attributeNamesWithStrings(Collections.singletonList("All")).build();
        Map<String, String> hostQueueAttributes = client.getQueueAttributes(hostGetQueueAttributes).attributesAsStrings();
        Assert.assertEquals(Long.toString(idleQueueRetentionPeriod), hostQueueAttributes.get(Constants.IDLE_QUEUE_RETENTION_PERIOD));
    }
}
