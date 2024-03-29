package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.util.IntegrationTest;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSMessageConsumerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AmazonSQSResponsesClientCrossAccountIT extends IntegrationTest {
    private static AmazonSQSRequester sqsRequester;
    private static AmazonSQSResponder sqsResponder;
    private static String requestQueueUrl;

    @Override
    protected String testSuiteName() {
        return "SQSXAccountResponsesClientIT";
    }

    @BeforeEach
    public void setup() {
        // Use the secondary role for the responder
        sqsResponder = new AmazonSQSResponderClient(getBuddyPrincipalClient());

        String policyString = allowSendMessagePolicy(getBuddyRoleARN()).toJson();
        sqsRequester = new AmazonSQSRequesterClient(sqs, queueNamePrefix,
                Collections.singletonMap(QueueAttributeName.POLICY.toString(), policyString),
                exceptionHandler);

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName("RequestQueue-" + UUID.randomUUID().toString()).build();
        requestQueueUrl = sqs.createQueue(createQueueRequest).queueUrl();
    }

    @AfterEach
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
