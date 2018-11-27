package com.amazonaws.services.sqs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.SendMessageRequest;

class AmazonSQSResponderClient implements AmazonSQSResponder {
    
    private static final Log LOG = LogFactory.getLog(AmazonSQSRequesterClient.class);

    private final AmazonSQS sqs;

    public AmazonSQSResponderClient(AmazonSQS sqs) {
        this.sqs = sqs;
    }

    @Override
    public AmazonSQS getAmazonSQS() {
        return sqs;
    }
    
    @Override
    public void sendResponseMessage(MessageContent request, MessageContent response) {
        MessageAttributeValue attribute = request.getMessageAttributes().get(AmazonSQSRequesterClient.RESPONSE_QUEUE_URL_ATTRIBUTE_NAME);

        if (attribute != null) {
            String replyQueueUrl = attribute.getStringValue();
            try {
                SendMessageRequest responseRequest = response.toSendMessageRequest()
                        .withQueueUrl(replyQueueUrl);
                sqs.sendMessage(responseRequest);
            } catch (QueueDoesNotExistException e) {
                // Stale request, ignore
                // TODO-RS: CW metric
                LOG.warn("Ignoring response to deleted response queue: " + replyQueueUrl);
            }
        } else {
            // TODO-RS: CW metric
            LOG.warn("Attempted to send response when none was requested");
        }
    }

    @Override
    public boolean isResponseMessageRequested(MessageContent requestMessage) {
        return requestMessage.getMessageAttributes().containsKey(AmazonSQSRequesterClient.RESPONSE_QUEUE_URL_ATTRIBUTE_NAME);
    }
    
    @Override
    public void shutdown() {
    }
}
