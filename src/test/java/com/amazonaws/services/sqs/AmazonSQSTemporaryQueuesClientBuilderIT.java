package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.util.IntegrationTest;

public class AmazonSQSTemporaryQueuesClientBuilderIT extends IntegrationTest {

    @Test
    public void supportsBothFeatures() {
        AmazonSQS sqs = AmazonSQSTemporaryQueuesClientBuilder.standard()
                .withQueuePrefix(queueNamePrefix)
                .build();
        String hostQueueUrl = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "Host")
                .addAttributesEntry("IdleQueueRetentionPeriodSeconds", "300")).getQueueUrl();
        GetQueueAttributesResult result = sqs.getQueueAttributes(hostQueueUrl, Collections.singletonList("IdleQueueRetentionPeriodSeconds"));
        assertEquals("300", result.getAttributes().get("IdleQueueRetentionPeriodSeconds"));
        
        String virtualQueueUrl = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "VirtualQueue")
                .addAttributesEntry("HostQueueUrl", hostQueueUrl)
                .addAttributesEntry("IdleQueueRetentionPeriodSeconds", "300")).getQueueUrl();
        
        result = sqs.getQueueAttributes(virtualQueueUrl, 
                Arrays.asList("HostQueueUrl", "IdleQueueRetentionPeriodSeconds"));
        assertEquals(hostQueueUrl, result.getAttributes().get("HostQueueUrl"));
        assertEquals("300", result.getAttributes().get("IdleQueueRetentionPeriodSeconds"));
        
    }
    
    @Test
    public void supportsTurningOffIdleQueueSweeping() {
        AmazonSQS sqs = AmazonSQSTemporaryQueuesClientBuilder.standard()
                .withQueuePrefix(queueNamePrefix)
                .withIdleQueueSweepingPeriod(0, TimeUnit.MINUTES)
                .build();
        String hostQueueUrl = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "Host")
                .addAttributesEntry("IdleQueueRetentionPeriodSeconds", "300")).getQueueUrl();
        GetQueueAttributesResult result = sqs.getQueueAttributes(hostQueueUrl, Collections.singletonList("IdleQueueRetentionPeriodSeconds"));
        assertEquals("300", result.getAttributes().get("IdleQueueRetentionPeriodSeconds"));
        
        String virtualQueueUrl = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "VirtualQueue")
                .addAttributesEntry("HostQueueUrl", hostQueueUrl)
                .addAttributesEntry("IdleQueueRetentionPeriodSeconds", "300")).getQueueUrl();
        
        result = sqs.getQueueAttributes(virtualQueueUrl, 
                Arrays.asList("HostQueueUrl", "IdleQueueRetentionPeriodSeconds"));
        assertEquals(hostQueueUrl, result.getAttributes().get("HostQueueUrl"));
        assertEquals("300", result.getAttributes().get("IdleQueueRetentionPeriodSeconds"));
    }
}
