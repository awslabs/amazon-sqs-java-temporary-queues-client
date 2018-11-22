package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;
import com.amazonaws.services.sqs.responsesapi.MessageContent;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSQueueUtils;

// TODO-RS: Configuration of queue attributes to use, or at least a policy
public class AmazonSQSResponsesClient implements AmazonSQSWithResponses {
    
    private static final Log LOG = LogFactory.getLog(AmazonSQSResponsesClient.class);

    public static final String RESPONSE_QUEUE_URL_ATTRIBUTE_NAME = "ResponseQueueUrl";
    
    private final AmazonSQS sqs;
    
    AmazonSQSResponsesClient(AmazonSQS sqs) {
        this.sqs = sqs;
    }
    
    public static AmazonSQSWithResponses make(AmazonSQS sqs) {
        return new AmazonSQSResponsesClient(AmazonSQSTemporaryQueuesClient.make(sqs));
    }
    
    public static AmazonSQSWithResponses make(AmazonSQS sqs, String clientID) {
        return new AmazonSQSResponsesClient(AmazonSQSTemporaryQueuesClient.make(sqs, clientID));
    }
    
    @Override
    public AmazonSQS getAmazonSQS() {
        return sqs;
    }
    
    @Override
	public Message sendMessageAndGetResponse(SendMessageRequest request, int timeout, TimeUnit unit) throws TimeoutException {
        return waitForFuture(sendMessageAndGetResponseAsync(request, timeout, unit), timeout, unit);
	}

	@Override
	public CompletableFuture<Message> sendMessageAndGetResponseAsync(SendMessageRequest request, int timeout, TimeUnit unit) {
	    String responseQueueUrl = sqs.createQueue(new CreateQueueRequest().withQueueName(UUID.randomUUID().toString())).getQueueUrl();

        SendMessageRequest requestWithResponseUrl = SQSQueueUtils.copyWithExtraAttributes(request,
                Collections.singletonMap(RESPONSE_QUEUE_URL_ATTRIBUTE_NAME, 
                        new MessageAttributeValue().withDataType("String").withStringValue(responseQueueUrl)));
        sqs.sendMessage(requestWithResponseUrl);
        
        CompletableFuture<Message> future = new CompletableFuture<>();
        // TODO-RS: accept an AmazonSQSAsync instead and use its threads instead of our own.
        // TODO-RS: complete the future exceptionally, for the right set of exceptions
        SQSMessageConsumer consumer = new SQSMessageConsumer(sqs, responseQueueUrl,
                future::complete, () -> future.completeExceptionally(new TimeoutException()));
        consumer.runFor(timeout, unit);
        future.whenComplete((message, exception) -> {
            consumer.shutdown();
            sqs.deleteQueue(responseQueueUrl);
        });
        return future;
	}

	/**
     * this method carefully waits for futures. If waiting throws, it converts the exceptions to the
     * exceptions that SQS clients expect. This is what we use to turn asynchronous calls into
     * synchronous ones.
     */
	// TODO-RS: Copied from QueueBuffer in the buffered asynchronous client
    private <ResultType> ResultType waitForFuture(Future<ResultType> future, long timeout, TimeUnit unit) throws TimeoutException {
        ResultType toReturn = null;
        try {
            toReturn = future.get(timeout, unit);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            AmazonClientException ce = new AmazonClientException(
                    "Thread interrupted while waiting for execution result");
            ce.initCause(ie);
            throw ce;
        } catch (ExecutionException ee) {
            // if the cause of the execution exception is an SQS exception, extract it
            // and throw the extracted exception to the clients
            // otherwise, wrap ee in an SQS exception and throw that.
            Throwable cause = ee.getCause();

            if (cause instanceof AmazonClientException) {
                throw (AmazonClientException) cause;
            }

            AmazonClientException ce = new AmazonClientException(
                    "Caught an exception while waiting for request to complete...");
            ce.initCause(ee);
            throw ce;
        }

        return toReturn;
    }

    @Override
	public void sendResponseMessage(MessageContent request, MessageContent response) {
	    MessageAttributeValue attribute = request.getMessageAttributes().get(RESPONSE_QUEUE_URL_ATTRIBUTE_NAME);
        
	    if (attribute != null) {
	        String replyQueueUrl = attribute.getStringValue();
	        try {
	            SendMessageRequest responseRequest = new SendMessageRequest()
	                    .withMessageBody(response.getMessageBody())
	                    .withMessageAttributes(response.getMessageAttributes())
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
	    return requestMessage.getMessageAttributes().containsKey(RESPONSE_QUEUE_URL_ATTRIBUTE_NAME);
	}
	
	public Message receiveResponse(String responseQueueUrl, int waitTimeSeconds) throws TimeoutException {
        try {
            ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                    .withQueueUrl(responseQueueUrl)
                    .withMaxNumberOfMessages(1)
                    .withWaitTimeSeconds(waitTimeSeconds);
            List<Message> messages = sqs.receiveMessage(receiveRequest).getMessages();
            if (messages.isEmpty()) {
                throw new TimeoutException();
            } else {
                return messages.get(0);
            }
        } finally {
            sqs.deleteQueue(responseQueueUrl);
        }
    }
	
	@Override
	public void shutdown() {
	    sqs.shutdown();
	}
}
