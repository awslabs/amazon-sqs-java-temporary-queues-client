package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.amazonaws.services.sqs.util.IntegrationTest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;

public class AmazonSQSTemporaryQueuesClientBuilderIT extends IntegrationTest {

    @Test
    public void supportsBothFeatures() {
        SqsClient sqs = AmazonSQSTemporaryQueuesClientBuilder.standard()
                .withQueuePrefix(queueNamePrefix)
                .build();
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put("IdleQueueRetentionPeriodSeconds", "300");
        String hostQueueUrl = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "Host")
                .attributesWithStrings(attributes).build()).queueUrl();
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(hostQueueUrl).attributeNamesWithStrings(Collections.singletonList("IdleQueueRetentionPeriodSeconds")).build();
        GetQueueAttributesResponse result = sqs.getQueueAttributes(getQueueAttributesRequest);
        assertEquals("300", result.attributesAsStrings().get("IdleQueueRetentionPeriodSeconds"));

        HashMap<String, String> virtualQueueattributes = new HashMap<>();
        virtualQueueattributes.put("IdleQueueRetentionPeriodSeconds", "300");
        virtualQueueattributes.put("HostQueueUrl", hostQueueUrl);
        String virtualQueueUrl = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "VirtualQueue")
                .attributesWithStrings(virtualQueueattributes).build()).queueUrl();

        GetQueueAttributesRequest virtualGetQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(virtualQueueUrl)
                .attributeNamesWithStrings(Arrays.asList("HostQueueUrl", "IdleQueueRetentionPeriodSeconds")).build();
        result = sqs.getQueueAttributes(virtualGetQueueAttributesRequest);
        assertEquals(hostQueueUrl, result.attributesAsStrings().get("HostQueueUrl"));
        assertEquals("300", result.attributesAsStrings().get("IdleQueueRetentionPeriodSeconds"));
    }
    
    @Test
    public void supportsTurningOffIdleQueueSweeping() {
        SqsClient sqs = AmazonSQSTemporaryQueuesClientBuilder.standard()
                .withQueuePrefix(queueNamePrefix)
                .withIdleQueueSweepingPeriod(0, TimeUnit.MINUTES)
                .build();
        HashMap<String, String> attributes = new HashMap<>();
        attributes.put("IdleQueueRetentionPeriodSeconds", "300");
        String hostQueueUrl = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "Host")
                .attributesWithStrings(attributes).build()).queueUrl();
        GetQueueAttributesRequest getQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(hostQueueUrl).attributeNamesWithStrings(Collections.singletonList("IdleQueueRetentionPeriodSeconds")).build();
        GetQueueAttributesResponse result = sqs.getQueueAttributes(getQueueAttributesRequest);
        assertEquals("300", result.attributesAsStrings().get("IdleQueueRetentionPeriodSeconds"));

        HashMap<String, String> virtualQueueattributes = new HashMap<>();
        virtualQueueattributes.put("IdleQueueRetentionPeriodSeconds", "300");
        virtualQueueattributes.put("HostQueueUrl", hostQueueUrl);
        String virtualQueueUrl = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "VirtualQueue")
                .attributesWithStrings(virtualQueueattributes).build()).queueUrl();

        GetQueueAttributesRequest virtualGetQueueAttributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(virtualQueueUrl)
                .attributeNamesWithStrings(Arrays.asList("HostQueueUrl", "IdleQueueRetentionPeriodSeconds")).build();
        result = sqs.getQueueAttributes(virtualGetQueueAttributesRequest);
        assertEquals(hostQueueUrl, result.attributesAsStrings().get("HostQueueUrl"));
        assertEquals("300", result.attributesAsStrings().get("IdleQueueRetentionPeriodSeconds"));
    }
}
