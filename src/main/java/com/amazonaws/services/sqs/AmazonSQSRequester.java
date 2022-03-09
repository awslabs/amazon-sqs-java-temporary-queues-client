package com.amazonaws.services.sqs;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface AmazonSQSRequester {

    SqsClient getAmazonSQS();

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

    public void shutdown();
}
