package com.amazonaws.services.sqs.util;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.auth.policy.Action;
import software.amazon.awssdk.core.auth.policy.Policy;
import software.amazon.awssdk.core.auth.policy.Principal;
import software.amazon.awssdk.core.auth.policy.Resource;
import software.amazon.awssdk.core.auth.policy.Statement;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

/**
 * Base class for integration tests
 */
public abstract class IntegrationTest {
    
    protected SqsClient sqs;
    // UUIDs are too long for this
    protected String queueNamePrefix = "__" + testSuiteName() + "-" + ThreadLocalRandom.current().nextInt(1000000);
    protected ExceptionAsserter exceptionHandler = new ExceptionAsserter();

    /**
     * Customizable to ensure queue names stay under 80 characters
     */
    protected String testSuiteName() {
        return getClass().getSimpleName();
    }

    @Before
    public void setupSQSClient() {
        sqs = SqsClient.create();
    }
    
    @After
    public void teardownSQSClient() {
        if (sqs != null) {
            // Best effort cleanup of queues. To be complete, we'd have to wait a minute
            // for the eventual consistency of listQueues()
            sqs.listQueues(ListQueuesRequest.builder().queueNamePrefix(queueNamePrefix).build()).queueUrls().forEach(queueUrl -> {
                try {
                    sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
                } catch (QueueDoesNotExistException e) {
                    // Ignore
                }
            });
            sqs.close();
        }
        exceptionHandler.assertNothingThrown();
    }

    protected String getBuddyRoleARN() {
        String roleARN = System.getenv("BUDDY_ROLE_ARN");
        if (roleARN == null) {
            assumeTrue("This test requires a second 'buddy' AWS role, provided with the BUDDY_ROLE_ARN environment variable.", false);
        }
        return roleARN;
    }

    protected AwsCredentialsProvider getBuddyCredentials() {
        StsClient stsClient = StsClient.builder().region(Region.US_WEST_2).build();
        return StsAssumeRoleCredentialsProvider
                .builder().stsClient(stsClient).refreshRequest(
                        AssumeRoleRequest.builder().roleArn(getBuddyRoleARN()).roleSessionName(testSuiteName()).build()
                ).build();
    }

    protected SqsClient getBuddyPrincipalClient() {
        AwsCredentialsProvider credentialsProvider = getBuddyCredentials();
        SqsClient client = SqsClient.builder()
                .region(Region.US_WEST_2)
                .credentialsProvider(credentialsProvider)
                .build();

        // Assume that the principal is not able to send messages to arbitrary queues
        String queueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(queueNamePrefix + "TestQueue").build()).queueUrl();
        try {
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody("Haxxors!!").build();
            client.sendMessage(sendMessageRequest);
            assumeTrue("The buddy credentials should not authorize sending to arbitrary queues", false);
        } catch (SqsException e) {
            // Access Denied
            assumeThat(e.statusCode(), equalTo(403));
        } finally {
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        }

        return client;
    }

    protected Policy allowSendMessagePolicy(String roleARN) {

        Policy policy = new Policy();
        Statement statement = new Statement(Statement.Effect.Allow);
        statement.setActions(Collections.singletonList(new Action("sqs:SendMessage")));
        statement.setPrincipals(new Principal(roleARN));
        statement.setResources(Collections.singletonList(new Resource("arn:aws:sqs:*:*:*")));
        policy.setStatements(Collections.singletonList(statement));
        return policy;
    }
}
