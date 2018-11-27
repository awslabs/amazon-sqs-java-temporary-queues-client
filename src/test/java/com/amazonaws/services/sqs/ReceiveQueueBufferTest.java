package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.util.ReceiveQueueBuffer;

public class ReceiveQueueBufferTest {

    private final AmazonSQS sqs;
    private final String queueUrl;
    
    public ReceiveQueueBufferTest() {
        this.sqs = mock(AmazonSQS.class);
        this.queueUrl = "http://queue.amazon.com/123456789012/MyQueue";
        
        Map<String, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(), "20");
        attributes.put(QueueAttributeName.VisibilityTimeout.toString(), "30");
        List<String> attributeNames = Arrays.asList(
                QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(),
                QueueAttributeName.VisibilityTimeout.toString());
        GetQueueAttributesResult getQueueAttributesResult = new GetQueueAttributesResult()
                .withAttributes(attributes);
        when(sqs.getQueueAttributes(
                eq(new GetQueueAttributesRequest()
                        .withQueueUrl(queueUrl)
                        .withAttributeNames(attributeNames))))
                .thenReturn(getQueueAttributesResult);
    }
    
    @Test
    public void deliverBeforeReceive() throws InterruptedException, ExecutionException, TimeoutException {
        ReceiveQueueBuffer buffer = new ReceiveQueueBuffer(sqs, queueUrl);
        Message message = new Message().withBody("Hi there!");
        buffer.deliverMessages(Collections.singletonList(message), queueUrl, null);
        Future<ReceiveMessageResult> future = buffer.receiveMessageAsync(new ReceiveMessageRequest());
        ReceiveMessageResult result = future.get(2, TimeUnit.SECONDS);
        assertEquals(1, result.getMessages().size());
    }
}
