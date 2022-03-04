package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.IntegrationTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class AmazonSQSTemporaryQueuesClientCrossAccountIT extends IntegrationTest {

    @Parameters(name= "With temporary queues = {0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] { { false }, { true } } );
    }

    // Parameterized to emphasize that the same client code for physical SQS queues
    // should work for temporary queues as well, even thought they are virtual.
    private final boolean withTemporaryQueues;
    private SqsClient client;
    private SqsClient otherAccountClient;

    public AmazonSQSTemporaryQueuesClientCrossAccountIT(boolean withTemporaryQueues) {
        this.withTemporaryQueues = withTemporaryQueues;
    }

    @Before
    public void setup() {
        client = makeTemporaryQueueClient(sqs);
        otherAccountClient = makeTemporaryQueueClient(getBuddyPrincipalClient());
    }

    @After
    public void teardown() {
        client.close();
    }

    private SqsClient makeTemporaryQueueClient(SqsClient sqs) {
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

    @Override
    protected String testSuiteName() {
        return "SQSXAccountTempQueueIT";
    }

    @Test
    public void accessDenied() {
        // Assert that a different principal is not permitted to
        // send to virtual queues
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "TestQueueWithoutAccess").build();
        String virtualQueueUrl = client.createQueue(createQueueRequest).queueUrl();
        try {
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(virtualQueueUrl).messageBody("Haxxors!!").build();
            otherAccountClient.sendMessage(sendMessageRequest);
            Assert.fail("Should not have been able to send a message");
        } catch (SqsException e) {
            // Access Denied
            assertEquals(403, e.statusCode());
        } finally {
            client.deleteQueue(DeleteQueueRequest.builder().queueUrl(virtualQueueUrl).build());
        }
    }

    @Test
    public void withAccess() {
        String policyString = allowSendMessagePolicy(getBuddyRoleARN()).toJson();
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueNamePrefix + "TestQueueWithAccess")
                .attributesWithStrings(Collections.singletonMap(QueueAttributeName.POLICY.name(), policyString)).build();

        String queueUrl = client.createQueue(createQueueRequest).queueUrl();
        try {
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl).messageBody("Hi there!").build();
            otherAccountClient.sendMessage(sendMessageRequest);
        } finally {
            client.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        }
    }

}
