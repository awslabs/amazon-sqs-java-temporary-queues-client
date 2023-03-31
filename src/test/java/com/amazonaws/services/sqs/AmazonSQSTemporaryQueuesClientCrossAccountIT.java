package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.IntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class AmazonSQSTemporaryQueuesClientCrossAccountIT extends IntegrationTest {
    private AmazonSQS client;
    private AmazonSQS otherAccountClient;

    @AfterEach
    public void teardown() {
        client.shutdown();
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
        String virtualQueueUrl = client.createQueue(queueNamePrefix + "TestQueueWithoutAccess").getQueueUrl();
        try {
            otherAccountClient.sendMessage(virtualQueueUrl, "Haxxors!!");
            fail("Should not have been able to send a message");
        } catch (AmazonSQSException e) {
            // Access Denied
            assertEquals(403, e.getStatusCode());
        } finally {
            client.deleteQueue(virtualQueueUrl);
        }
    }

    @ParameterizedTest
    @MethodSource("data")
    public void withAccess(boolean withTemporaryQueues) {
        init(withTemporaryQueues);
        String policyString = allowSendMessagePolicy(getBuddyRoleARN()).toJson();
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "TestQueueWithAccess")
                .withAttributes(Collections.singletonMap(QueueAttributeName.Policy.toString(), policyString));

        String queueUrl = client.createQueue(createQueueRequest).getQueueUrl();
        try {
            otherAccountClient.sendMessage(queueUrl, "Hi there!");
        } finally {
            client.deleteQueue(queueUrl);
        }
    }

    private static List<Arguments> data() {
        return List.of(Arguments.of(false), Arguments.of(true));
    }

    public void init(boolean withTemporaryQueues) {
        client = makeTemporaryQueueClient(sqs, withTemporaryQueues);
        otherAccountClient = makeTemporaryQueueClient(getBuddyPrincipalClient(), withTemporaryQueues);
    }

    private AmazonSQS makeTemporaryQueueClient(AmazonSQS sqs, boolean withTemporaryQueues) {
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
