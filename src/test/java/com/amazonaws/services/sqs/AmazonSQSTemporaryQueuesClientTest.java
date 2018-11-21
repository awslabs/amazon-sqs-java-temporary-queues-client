package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;
import com.amazonaws.services.sqs.responsesapi.MessageContent;

public class AmazonSQSTemporaryQueuesClientTest {
	private static AmazonSQS sqs;
    private static AmazonSQSWithResponses rpcClient;
    private static List<SQSExecutorService> executors = new ArrayList<>();
    private static List<Throwable> taskExceptions = new ArrayList<>();
    
    @Before
    public void setup() {

//        final AWSCredentialsProvider credentialsProvider = new OdinAWSCredentialsProvider(
//            TestUtils.getOdinMaterialSet());
        
        sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
//                .withCredentials(credentialsProvider)
                .build();
        rpcClient = AmazonSQSResponsesClient.make(sqs, "AmazonSQSTemporaryQueuesClientTest");
        executors.clear();
        taskExceptions.clear();
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
