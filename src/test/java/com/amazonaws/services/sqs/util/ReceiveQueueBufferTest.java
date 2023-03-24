package com.amazonaws.services.sqs.util;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReceiveQueueBufferTest {

    private final SqsClient sqs;
    private final ScheduledExecutorService executor;
    private final String queueUrl;
    
    public ReceiveQueueBufferTest() {
        this.sqs = mock(SqsClient.class);
        this.executor = mock(ScheduledExecutorService.class);
        this.queueUrl = "http://queue.amazon.com/123456789012/MyQueue";
        
        Map<String, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS.toString(), "20");
        attributes.put(QueueAttributeName.VISIBILITY_TIMEOUT.toString(), "30");
        List<String> attributeNames = Arrays.asList(
                QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS.toString(),
                QueueAttributeName.VISIBILITY_TIMEOUT.toString());
        GetQueueAttributesResponse getQueueAttributesResult = GetQueueAttributesResponse.builder()
                .attributesWithStrings(attributes).build();
        when(sqs.getQueueAttributes(
                eq(GetQueueAttributesRequest.builder()
                        .queueUrl(queueUrl)
                        .attributeNamesWithStrings(attributeNames).build())))
                .thenReturn(getQueueAttributesResult);
    }
    
    @Test
    public void deliverBeforeReceive() throws InterruptedException, ExecutionException, TimeoutException {
        ReceiveQueueBuffer buffer = new ReceiveQueueBuffer(sqs, executor, queueUrl);
        Message message = Message.builder().body("Hi there!").build();
        buffer.deliverMessages(Collections.singletonList(message), queueUrl, null);
        Future<ReceiveMessageResponse> future = buffer.receiveMessageAsync(ReceiveMessageRequest.builder().build());
        ReceiveMessageResponse result = future.get(2, TimeUnit.SECONDS);
        assertEquals(1, result.messages().size());
    }
    
    @Test
    public void deliverAfterReceive() throws InterruptedException, ExecutionException, TimeoutException {
        ReceiveQueueBuffer buffer = new ReceiveQueueBuffer(sqs, executor, queueUrl);
        Message message = Message.builder().body("Hi there!").build();
        Future<ReceiveMessageResponse> future = buffer.receiveMessageAsync(ReceiveMessageRequest.builder().build());
        buffer.deliverMessages(Collections.singletonList(message), queueUrl, null);
        ReceiveMessageResponse result = future.get(2, TimeUnit.SECONDS);
        assertEquals(1, result.messages().size());
    }
}
