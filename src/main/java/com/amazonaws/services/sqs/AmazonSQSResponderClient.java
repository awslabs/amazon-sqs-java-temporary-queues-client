package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.util.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

class AmazonSQSResponderClient implements AmazonSQSResponder {
    
    private static final Log LOG = LogFactory.getLog(AmazonSQSResponderClient.class);

    private final SqsClient sqs;

    public AmazonSQSResponderClient(SqsClient sqs) {
        this.sqs = sqs;
    }

    @Override
    public SqsClient getAmazonSQS() {
        return sqs;
    }
    
    @Override
    public void sendResponseMessage(MessageContent request, MessageContent response) {
        MessageAttributeValue attribute = request.getMessageAttributes().get(Constants.RESPONSE_QUEUE_URL_ATTRIBUTE_NAME);

        if (attribute != null) {
            String replyQueueUrl = attribute.stringValue();
            try {
                SendMessageRequest responseRequest = response.toSendMessageRequest().toBuilder()
                        .queueUrl(replyQueueUrl).build();
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
        return requestMessage.getMessageAttributes().containsKey(Constants.RESPONSE_QUEUE_URL_ATTRIBUTE_NAME);
    }
    
    @Override
    public void shutdown() {
    }
}
