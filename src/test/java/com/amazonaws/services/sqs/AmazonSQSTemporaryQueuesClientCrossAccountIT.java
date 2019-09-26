package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.IntegrationTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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
    private AmazonSQS client;
    private AmazonSQS otherAccountClient;

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
        client.shutdown();
    }

    private AmazonSQS makeTemporaryQueueClient(AmazonSQS sqs) {
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
        String virtualQueueUrl = client.createQueue(queueNamePrefix + "TestQueueWithoutAccess").getQueueUrl();
        try {
            otherAccountClient.sendMessage(virtualQueueUrl, "Haxxors!!");
            Assert.fail("Should not have been able to send a message");
        } catch (AmazonSQSException e) {
            // Access Denied
            assertEquals(403, e.getStatusCode());
        } finally {
            client.deleteQueue(virtualQueueUrl);
        }
    }

    @Test
    public void withAccess() {
        String policyString = allowSendMessagePolicy().toJson();
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

}
