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
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.TestUtils;

public class AmazonSQSResponsesClientTest extends TestUtils {
    private static AmazonSQS sqs;
    private static AmazonSQSRequester sqsRequester;
    private static AmazonSQSResponder sqsResponder;
    private static String requestQueueUrl;


    @Before
    public void setup() {
        sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
        sqsRequester = new AmazonSQSRequesterClient(sqs, AmazonSQSResponsesClientTest.class.getSimpleName());
        sqsResponder = new AmazonSQSResponderClient(sqs);
        requestQueueUrl = sqs.createQueue("RequestQueue-" + UUID.randomUUID().toString()).getQueueUrl();
    }

    @After
    public void teardown() throws InterruptedException {
        sqs.deleteQueue(requestQueueUrl);
        sqsResponder.shutdown();
        sqsRequester.shutdown();
        sqs.shutdown();
    }

    @Test
    public void test() throws Exception {
        new SQSMessageConsumer(sqs, requestQueueUrl, message -> {
            sqsResponder.sendResponseMessage(MessageContent.fromMessage(message),
                    new MessageContent("Right back atcha buddy!"));
        }).start();

        SendMessageRequest request = new SendMessageRequest()
                .withMessageBody("Hi there!")
                .withQueueUrl(requestQueueUrl);
        Message replyMessage = sqsRequester.sendMessageAndGetResponse(request, 5, TimeUnit.SECONDS);

        assertEquals("Right back atcha buddy!", replyMessage.getBody());
    }
}
