package com.amazonaws.services.sqs.util;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.function.BiFunction;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.AddPermissionRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ListDeadLetterSourceQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.RemovePermissionRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.SqsRequest;
import software.amazon.awssdk.services.sqs.model.TagQueueRequest;
import software.amazon.awssdk.services.sqs.model.UntagQueueRequest;

public class AbstractAmazonSQSClientWrapperTest {

    private SqsClient wrapped = Mockito.mock(SqsClient.class);
    private SqsClient wrapper = new Wrapper(wrapped);
    
    private static class Wrapper extends AbstractAmazonSQSClientWrapper {

        public Wrapper(SqsClient amazonSqsToBeExtended) {
            super(amazonSqsToBeExtended, "WrapperTest");
        }
        
    }
    
    @Test
    public void addPermission() {
        assertWrappedMethod(SqsClient::addPermission, AddPermissionRequest.builder().build());
    }
    
    @Test
    public void changeMessageVisibility() {
        assertWrappedMethod(SqsClient::changeMessageVisibility, ChangeMessageVisibilityRequest.builder().build());
    }
    
    @Test
    public void changeMessageVisibilityBatch() {
        assertWrappedMethod(SqsClient::changeMessageVisibilityBatch, ChangeMessageVisibilityBatchRequest.builder().build());
    }
    
    @Test
    public void createQueue() {
        assertWrappedMethod(SqsClient::createQueue, CreateQueueRequest.builder().build());
    }
    
    @Test
    public void deleteMessage() {
        assertWrappedMethod(SqsClient::deleteMessage, DeleteMessageRequest.builder().build());
    }
    
    @Test
    public void deleteQueue() {
        assertWrappedMethod(SqsClient::deleteQueue, DeleteQueueRequest.builder().build());
    }
    
    @Test
    public void deleteMessageBatch() {
        assertWrappedMethod(SqsClient::deleteMessageBatch, DeleteMessageBatchRequest.builder().build());
    }
    
    @Test
    public void getQueueAttributes() {
        assertWrappedMethod(SqsClient::getQueueAttributes, GetQueueAttributesRequest.builder().build());
    }
    
    @Test
    public void getQueueUrl() {
        assertWrappedMethod(SqsClient::getQueueUrl, GetQueueUrlRequest.builder().build());
    }
    
    @Test
    public void listDeadLetterSourceQueues() {
        assertWrappedMethod(SqsClient::listDeadLetterSourceQueues, ListDeadLetterSourceQueuesRequest.builder().build());
    }
    
    @Test
    public void listQueues() {
        assertWrappedMethod(SqsClient::listQueues, ListQueuesRequest.builder().build());
    }
    
    @Test
    public void listQueueTags() {
        assertWrappedMethod(SqsClient::listQueueTags, ListQueueTagsRequest.builder().build());
    }
    
    @Test
    public void purgeQueue() {
        assertWrappedMethod(SqsClient::purgeQueue, PurgeQueueRequest.builder().build());
    }
    
    @Test
    public void receiveMessage() {
        assertWrappedMethod(SqsClient::receiveMessage, ReceiveMessageRequest.builder().build());
    }
    
    @Test
    public void removePermission() {
        assertWrappedMethod(SqsClient::removePermission, RemovePermissionRequest.builder().build());
    }
    
    @Test
    public void sendMessage() {
        assertWrappedMethod(SqsClient::sendMessage, SendMessageRequest.builder().build());
    }
    
    @Test
    public void sendMessageBatch() {
        assertWrappedMethod(SqsClient::sendMessageBatch, SendMessageBatchRequest.builder().build());
    }
    
    @Test
    public void setQueueAttributes() {
        assertWrappedMethod(SqsClient::setQueueAttributes, SetQueueAttributesRequest.builder().build());
    }
    
    @Test
    public void tagQueue() {
        assertWrappedMethod(SqsClient::tagQueue, TagQueueRequest.builder().build());
    }
    
    @Test
    public void untagQueue() {
        assertWrappedMethod(SqsClient::untagQueue, UntagQueueRequest.builder().build());
    }
    
    @Test
    public void close() {
        wrapper.close();
        verify(wrapped, never()).close();
    }
    
    private <T extends SqsRequest> void assertWrappedMethod(BiFunction<SqsClient, T, ?> method, T request) {
        method.apply(wrapper, request);

        ArgumentCaptor<SqsRequest> argumentCaptor = ArgumentCaptor.forClass(request.getClass());
        method.apply(verify(wrapped), (T) argumentCaptor.capture());
        SqsRequest value = argumentCaptor.getValue();

        Assert.assertEquals(request.getClass(), value.getClass());
        Assert.assertTrue(value.overrideConfiguration().get().apiNames().get(0).name().equals("WrapperTest"));
    }
}
