package com.amazonaws.services.sqs.util;


import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.RequestClientOptions.Marker;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueueTagsRequest;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.RemovePermissionRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.TagQueueRequest;
import com.amazonaws.services.sqs.model.UntagQueueRequest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AbstractAmazonSQSClientWrapperTest {

    private AmazonSQS wrapped = Mockito.mock(AmazonSQS.class);
    private AmazonSQS wrapper = new Wrapper(wrapped);
    
    private static class Wrapper extends AbstractAmazonSQSClientWrapper {

        public Wrapper(AmazonSQS amazonSqsToBeExtended) {
            super(amazonSqsToBeExtended, "WrapperTest");
        }
        
    }
    
    @Test
    public void addPermission() {
        assertWrappedMethod(AmazonSQS::addPermission, new AddPermissionRequest());
    }
    
    @Test
    public void changeMessageVisibility() {
        assertWrappedMethod(AmazonSQS::changeMessageVisibility, new ChangeMessageVisibilityRequest());
    }
    
    @Test
    public void changeMessageVisibilityBatch() {
        assertWrappedMethod(AmazonSQS::changeMessageVisibilityBatch, new ChangeMessageVisibilityBatchRequest());
    }
    
    @Test
    public void createQueue() {
        assertWrappedMethod(AmazonSQS::createQueue, new CreateQueueRequest());
    }
    
    @Test
    public void deleteMessage() {
        assertWrappedMethod(AmazonSQS::deleteMessage, new DeleteMessageRequest());
    }
    
    @Test
    public void deleteQueue() {
        assertWrappedMethod(AmazonSQS::deleteQueue, new DeleteQueueRequest());
    }
    
    @Test
    public void deleteMessageBatch() {
        assertWrappedMethod(AmazonSQS::deleteMessageBatch, new DeleteMessageBatchRequest());
    }
    
    @Test
    public void getQueueAttributes() {
        assertWrappedMethod(AmazonSQS::getQueueAttributes, new GetQueueAttributesRequest());
    }
    
    @Test
    public void getQueueUrl() {
        assertWrappedMethod(AmazonSQS::getQueueUrl, new GetQueueUrlRequest());
    }
    
    @Test
    public void listDeadLetterSourceQueues() {
        assertWrappedMethod(AmazonSQS::listDeadLetterSourceQueues, new ListDeadLetterSourceQueuesRequest());
    }
    
    @Test
    public void listQueues() {
        assertWrappedMethod(AmazonSQS::listQueues, new ListQueuesRequest());
    }
    
    @Test
    public void listQueueTags() {
        assertWrappedMethod(AmazonSQS::listQueueTags, new ListQueueTagsRequest());
    }
    
    @Test
    public void purgeQueue() {
        assertWrappedMethod(AmazonSQS::purgeQueue, new PurgeQueueRequest());
    }
    
    @Test
    public void receiveMessage() {
        assertWrappedMethod(AmazonSQS::receiveMessage, new ReceiveMessageRequest());
    }
    
    @Test
    public void removePermission() {
        assertWrappedMethod(AmazonSQS::removePermission, new RemovePermissionRequest());
    }
    
    @Test
    public void sendMessage() {
        assertWrappedMethod(AmazonSQS::sendMessage, new SendMessageRequest());
    }
    
    @Test
    public void sendMessageBatch() {
        assertWrappedMethod(AmazonSQS::sendMessageBatch, new SendMessageBatchRequest());
    }
    
    @Test
    public void setQueueAttributes() {
        assertWrappedMethod(AmazonSQS::setQueueAttributes, new SetQueueAttributesRequest());
    }
    
    @Test
    public void tagQueue() {
        assertWrappedMethod(AmazonSQS::tagQueue, new TagQueueRequest());
    }
    
    @Test
    public void untagQueue() {
        assertWrappedMethod(AmazonSQS::untagQueue, new UntagQueueRequest());
    }
    
    @Test
    public void getCachedResponseMetadata() {
        ListQueuesRequest request = new ListQueuesRequest();
        wrapper.getCachedResponseMetadata(request);
        verify(wrapped).getCachedResponseMetadata(request);
    }
    
    @Test
    public void setEndpoint() {
        wrapper.setEndpoint("foobar.com");
        verify(wrapped).setEndpoint("foobar.com");
    }
    
    @Test
    public void shutdown() {
        wrapper.shutdown();
        verify(wrapped, never()).shutdown();
    }
    
    private <T extends AmazonWebServiceRequest> void assertWrappedMethod(BiFunction<AmazonSQS, T, ?> method, T request) {
        method.apply(wrapper, request);
        
        method.apply(verify(wrapped), request);
        
        assertTrue(request.getRequestClientOptions().getClientMarker(Marker.USER_AGENT).contains("WrapperTest"));
    }
}
