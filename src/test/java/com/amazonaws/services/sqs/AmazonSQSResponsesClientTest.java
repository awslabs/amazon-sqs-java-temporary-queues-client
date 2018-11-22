package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;
import com.amazonaws.services.sqs.responsesapi.MessageContent;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.TestUtils;

public class AmazonSQSResponsesClientTest extends TestUtils {
	private static AmazonSQS sqs;
    private static AmazonSQSWithResponses rpcClient;
    
    @Before
    public void setup() {
        sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();
        rpcClient = AmazonSQSResponsesClient.make(sqs, "AmazonSQSResponsesClientTest");
    }
    
    @After
    public void teardown() throws InterruptedException {
        rpcClient.shutdown();
    }
    
    @Test
    public void test() throws Exception {
    	String requestQueueUrl = sqs.createQueue("RequestQueue-" + UUID.randomUUID().toString()).getQueueUrl();
    	
    	new SQSMessageConsumer(sqs, requestQueueUrl, message -> {
    		rpcClient.sendResponseMessage(MessageContent.fromMessage(message),
    		                              new MessageContent("Right back atcha buddy!"));
    	}).start();
    	
    	SendMessageRequest request = new SendMessageRequest()
    			.withMessageBody("Hi there!")
    			.withQueueUrl(requestQueueUrl);
    	Message replyMessage = rpcClient.sendMessageAndGetResponse(request, 5, TimeUnit.SECONDS);
    	
    	assertEquals("Right back atcha buddy!", replyMessage.getBody());
    }
}
