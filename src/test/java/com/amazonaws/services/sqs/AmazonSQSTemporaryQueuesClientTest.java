package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class AmazonSQSTemporaryQueuesClientTest {
	private static AmazonSQS sqs;
    private static AmazonSQSTemporaryQueuesClient rpcClient;
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
        rpcClient = new AmazonSQSTemporaryQueuesClient(sqs, "AmazonSQSTemporaryQueuesClientTest");
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
    	
    	new SQSMessageConsumer(rpcClient, requestQueueUrl, message -> {
    		String replyQueueUrl = SQSQueueUtils.responseQueueUrl(message);
    		SendMessageRequest request = new SendMessageRequest()
    			.withQueueUrl(replyQueueUrl)
				.withMessageBody("Right back atcha buddy!");
    		SQSQueueUtils.sendResponse(rpcClient, request);
    	}).start();
    	
    	SendMessageRequest request = new SendMessageRequest()
    			.withMessageBody("Hi there!")
    			.withQueueUrl(requestQueueUrl);
    	String replyQueueUrl = SQSQueueUtils.sendMessageWithResponseQueue(rpcClient, request);
    	
    	ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
				.withQueueUrl(replyQueueUrl)
				.withWaitTimeSeconds(5);
		List<Message> messages = rpcClient.receiveMessage(receiveRequest).getMessages();
		
		assertEquals(1, messages.size());
    	assertEquals("Right back atcha buddy!", messages.get(0).getBody());
		
    	rpcClient.deleteQueue(replyQueueUrl);
    }
}
