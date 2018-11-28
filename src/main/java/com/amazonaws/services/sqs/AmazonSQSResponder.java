package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.AmazonSQS;

public interface AmazonSQSResponder {
    
    AmazonSQS getAmazonSQS();

    /**
     * Tests whether the given message was sent using
     * {@link AmazonSQSRequester#sendMessageAndGetResponse} or
     * {@link AmazonSQSRequester#sendMessageAndGetResponseAsync}.
     */
    public boolean isResponseMessageRequested(MessageContent requestMessage);

    /**
     * Given a message that was sent using 
     * {@link AmazonSQSRequester#sendMessageAndGetResponse} or
     * {@link AmazonSQSRequester#sendMessageAndGetResponseAsync},
     * sends the given message as its response.
     */
    public void sendResponseMessage(MessageContent requestMessage, MessageContent response);

    public void shutdown();
}
