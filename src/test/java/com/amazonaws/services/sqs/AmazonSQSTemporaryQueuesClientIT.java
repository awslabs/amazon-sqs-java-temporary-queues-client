package com.amazonaws.services.sqs;

import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.util.IntegrationTest;

public class AmazonSQSTemporaryQueuesClientIT extends IntegrationTest {

    private AmazonSQSTemporaryQueuesClient client;
    private String queueUrl;
    
    @Before
    public void setup() {
        client = AmazonSQSTemporaryQueuesClient.makeWrappedClient(sqs, queueNamePrefix);
    }
    
    @After
    public void teardown() {
        client.deleteQueue(queueUrl);
        client.shutdown();
    }
    
    @Test
    public void createQueueAddsAttributes() {
        queueUrl = client.createQueue("TestQueue").getQueueUrl();
        Map<String, String> attributes = client.getQueueAttributes(queueUrl, Collections.singletonList("All")).getAttributes();
        String hostQueueUrl = attributes.get(AmazonSQSVirtualQueuesClient.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);
        assertNotNull(hostQueueUrl);
        Assert.assertEquals("300", attributes.get(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD));
        
        Map<String, String> hostQueueAttributes = client.getQueueAttributes(queueUrl, Collections.singletonList("All")).getAttributes();
        Assert.assertEquals("300", hostQueueAttributes.get(AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD));
    }
}
