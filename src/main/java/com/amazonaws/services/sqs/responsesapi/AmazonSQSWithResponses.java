package com.amazonaws.services.sqs.responsesapi;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public interface AmazonSQSWithResponses {

    AmazonSQS getAmazonSQS();
    
    /**
     * Sends a message and waits the given amount of time for
     * the response message.
     */
    public Message sendMessageAndGetResponse(SendMessageRequest request, 
            int timeout, TimeUnit unit) throws TimeoutException;

    /**
     * Sends a message and returns a <tt>CompletableFuture</tt> 
     * that will be completed with the response message when it arrives.
     */
    public CompletableFuture<Message> sendMessageAndGetResponseAsync(SendMessageRequest request, 
            int timeout, TimeUnit unit);

    public boolean isResponseMessageRequested(MessageContent requestMessage);
    
    /**
     * Given a message that was sent using sendMessageAndGetResponse[Async],
     * sends the given message as its response.
     */
    public void sendResponseMessage(MessageContent requestMessage, MessageContent response);
    
    public void shutdown();
}
