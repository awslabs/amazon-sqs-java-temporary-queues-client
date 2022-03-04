package com.amazonaws.services.sqs;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.util.SQSMessageConsumerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.util.IntegrationTest;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class AmazonSQSResponsesClientIT extends IntegrationTest {
    private static AmazonSQSRequester sqsRequester;
    private static AmazonSQSResponder sqsResponder;
    private static String requestQueueUrl;


    @Before
    public void setup() {
        sqsRequester = new AmazonSQSRequesterClient(sqs, queueNamePrefix,
                Collections.emptyMap(), exceptionHandler);
        sqsResponder = new AmazonSQSResponderClient(sqs);
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName("RequestQueue-" + UUID.randomUUID().toString()).build();
        requestQueueUrl = sqs.createQueue(createQueueRequest).queueUrl();
    }

    @After
    public void teardown() {
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(requestQueueUrl).build());
        sqsResponder.shutdown();
        sqsRequester.shutdown();
    }

    @Test
    public void test() throws Exception {
        SQSMessageConsumer consumer = SQSMessageConsumerBuilder.standard()
                .withAmazonSQS(sqs)
                .withQueueUrl(requestQueueUrl)
                .withConsumer(message -> {
                    sqsResponder.sendResponseMessage(MessageContent.fromMessage(message),
                            new MessageContent("Right back atcha buddy!"));
                })
                .build();
        consumer.start();
        try {
            SendMessageRequest request = SendMessageRequest.builder()
                    .messageBody("Hi there!")
                    .queueUrl(requestQueueUrl).build();
            Message replyMessage = sqsRequester.sendMessageAndGetResponse(request, 5, TimeUnit.SECONDS);
    
            assertEquals("Right back atcha buddy!", replyMessage.body());
        } finally {
            consumer.terminate();
        }
    }
}
