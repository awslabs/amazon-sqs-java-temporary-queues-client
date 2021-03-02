package com.amazonaws.services.sqs;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazonaws.services.sqs.model.ListQueueTagsResult;
import org.junit.After;
import org.junit.Test;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.util.ExceptionAsserter;
import com.amazonaws.services.sqs.util.MockSQS;
import com.amazonaws.services.sqs.util.SQSQueueUtils;

public class AmazonSQSRequesterClientTest {
    
    private final ExceptionAsserter exceptionHandler = new ExceptionAsserter();
    
    private final AmazonSQS sqs;
    private final String accountPrefix;
    private final AmazonSQSRequesterClient requesterClient;
    private final AmazonSQSResponderClient responderClient;
    
    public AmazonSQSRequesterClientTest() {
        this.accountPrefix = "http://queue.amazon.com/123456789012/";
        this.sqs = new MockSQS(accountPrefix);
        this.requesterClient = new AmazonSQSRequesterClient(sqs, "RequesterClientQueues",
                                                            Collections.emptyMap(),
                                                            Collections.emptyMap(),
                                                            exceptionHandler);
        this.responderClient = new AmazonSQSResponderClient(sqs);
    }
    
    @After
    public void tearDown() {
        this.exceptionHandler.assertNothingThrown();
    }
    
    @Test
    public void happyPath() throws TimeoutException, InterruptedException, ExecutionException {
        String requestMessageBody = "Ping";
        String responseMessageBody = "Pong";
        
        String queueUrl = sqs.createQueue("MyQueue").getQueueUrl();
        
        SendMessageRequest request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(requestMessageBody);
        Future<Message> future = requesterClient.sendMessageAndGetResponseAsync(request, 5, TimeUnit.SECONDS);     
        
        Message requestMessage = sqs.receiveMessage(queueUrl).getMessages().get(0);
        assertEquals(requestMessageBody, requestMessage.getBody());
        String responseQueueUrl = requestMessage.getMessageAttributes().get(AmazonSQSRequesterClient.RESPONSE_QUEUE_URL_ATTRIBUTE_NAME).getStringValue();
        assertNotNull(responseQueueUrl);
        ListQueueTagsResult listQueueTagsResult = sqs.listQueueTags(responseQueueUrl);

        responderClient.sendResponseMessage(MessageContent.fromMessage(requestMessage), new MessageContent(responseMessageBody));
        
        Message response = future.get(5, TimeUnit.SECONDS);
        assertEquals(responseMessageBody, response.getBody());
        
        // Make sure the response queue gets deleted
        SQSQueueUtils.awaitQueueDeleted(sqs, responseQueueUrl, 70, TimeUnit.SECONDS);
    }

    @Test
    public void timeout() throws TimeoutException, InterruptedException, ExecutionException {
        String requestMessageBody = "Ping";
        
        String queueUrl = sqs.createQueue("MyQueue").getQueueUrl();
        
        SendMessageRequest request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(requestMessageBody);
        Future<Message> future = requesterClient.sendMessageAndGetResponseAsync(request, 1, TimeUnit.SECONDS);
        
        Message requestMessage = sqs.receiveMessage(queueUrl).getMessages().get(0);
        assertEquals(requestMessageBody, requestMessage.getBody());
        String responseQueueUrl = requestMessage.getMessageAttributes().get(AmazonSQSRequesterClient.RESPONSE_QUEUE_URL_ATTRIBUTE_NAME).getStringValue();
        assertNotNull(responseQueueUrl);
        // TODO-RS: Junit 5
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(TimeoutException.class));
        }
        
        // Make sure the response queue was deleted
        SQSQueueUtils.awaitQueueDeleted(sqs, responseQueueUrl, 70, TimeUnit.SECONDS);
    }
}
