package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.util.IntegrationTest;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSMessageConsumerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
                Collections.singletonMap(QueueAttributeName.Policy.toString(), policyString),
                exceptionHandler);

        requestQueueUrl = sqs.createQueue("RequestQueue-" + UUID.randomUUID()).getQueueUrl();
    }

    @AfterEach
    public void teardown() {
        sqs.deleteQueue(requestQueueUrl);
        sqsResponder.shutdown();
        sqsRequester.shutdown();
    }

    @Test
    public void test() throws Exception {
        SQSMessageConsumer consumer = SQSMessageConsumerBuilder.standard()
                .withAmazonSQS(sqs)
                .withQueueUrl(requestQueueUrl)
                .withConsumer(message -> sqsResponder.sendResponseMessage(MessageContent.fromMessage(message),
                        new MessageContent("Right back atcha buddy!")))
                .build();
        consumer.start();
        try {
            SendMessageRequest request = new SendMessageRequest()
                    .withMessageBody("Hi there!")
                    .withQueueUrl(requestQueueUrl);
            Message replyMessage = sqsRequester.sendMessageAndGetResponse(request, 5, TimeUnit.SECONDS);
    
            assertEquals("Right back atcha buddy!", replyMessage.getBody());
        } finally {
            consumer.terminate();
        }
    }
}
