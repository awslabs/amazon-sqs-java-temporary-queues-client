package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.IntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


public class AmazonSQSTemporaryQueuesClientCrossAccountIT extends IntegrationTest {
    private SqsClient client;
    private SqsClient otherAccountClient;

    @BeforeEach
    public void setup() {
        sqs = SqsClient.create();
    }

    @AfterEach
    public void teardown() {
        client.close();
    }

    @Override
    protected String testSuiteName() {
        return "SQSXAccountTempQueueIT";
    }

    @ParameterizedTest
    @MethodSource("data")
    public void accessDenied(boolean withTemporaryQueues) {
        init(withTemporaryQueues);
        // Assert that a different principal is not permitted to
        // send to virtual queues
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "TestQueueWithoutAccess").build();
        String virtualQueueUrl = client.createQueue(createQueueRequest).queueUrl();
        try {
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(virtualQueueUrl).messageBody("Haxxors!!").build();
            otherAccountClient.sendMessage(sendMessageRequest);
            fail("Should not have been able to send a message");
        } catch (SqsException e) {
            // Access Denied
            assertEquals(403, e.statusCode());
        } finally {
            client.deleteQueue(DeleteQueueRequest.builder().queueUrl(virtualQueueUrl).build());
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void withAccess(boolean withTemporaryQueues) {
        init(withTemporaryQueues);
        String policyString = allowSendMessagePolicy(getBuddyRoleARN()).toJson();
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "TestQueueWithAccess")
                .attributesWithStrings(Collections.singletonMap(QueueAttributeName.POLICY.toString(), policyString)).build();

        String queueUrl = client.createQueue(createQueueRequest).queueUrl();
        try {
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl).messageBody("Hi there!").build();
            otherAccountClient.sendMessage(sendMessageRequest);
        } finally {
            client.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        }
    }

    private static List<Arguments> data() {
        return List.of(Arguments.of(false), Arguments.of(true));
    }

    public void init(boolean withTemporaryQueues) {
        client = makeTemporaryQueueClient(sqs, withTemporaryQueues);
        otherAccountClient = makeTemporaryQueueClient(getBuddyPrincipalClient(), withTemporaryQueues);
    }

    private SqsClient makeTemporaryQueueClient(SqsClient sqs, boolean withTemporaryQueues) {
        if (withTemporaryQueues) {
            AmazonSQSRequesterClientBuilder requesterBuilder =
                    AmazonSQSRequesterClientBuilder.standard()
                            .withAmazonSQS(sqs)
                            .withInternalQueuePrefix(queueNamePrefix)
                            .withIdleQueueSweepingPeriod(0, TimeUnit.SECONDS);
            return AmazonSQSTemporaryQueuesClient.make(requesterBuilder);
        } else {
            // Use a wrapper just to avoid shutting down the client from
            // the base class too early.
            return new AbstractAmazonSQSClientWrapper(sqs);
        }
    }
}
