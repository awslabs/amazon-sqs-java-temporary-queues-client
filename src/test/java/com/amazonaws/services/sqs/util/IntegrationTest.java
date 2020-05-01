package com.amazonaws.services.sqs.util;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import org.junit.After;
import org.junit.Before;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

/**
 * Base class for integration tests
 */
public abstract class IntegrationTest {
    
    protected AmazonSQS sqs;
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
        sqs = AmazonSQSClientBuilder.defaultClient();
    }
    
    @After
    public void teardownSQSClient() {
        if (sqs != null) {
            // Best effort cleanup of queues. To be complete, we'd have to wait a minute
            // for the eventual consistency of listQueues()
            sqs.listQueues(queueNamePrefix).getQueueUrls().forEach(queueUrl -> {
                try {
                    sqs.deleteQueue(queueUrl);
                } catch (QueueDoesNotExistException e) {
                    // Ignore
                }
            });
            sqs.shutdown();
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

    protected AWSCredentialsProvider getBuddyCredentials() {
        return new STSAssumeRoleSessionCredentialsProvider.Builder(getBuddyRoleARN(), testSuiteName()).build();
    }

    protected AmazonSQS getBuddyPrincipalClient() {
        AWSCredentialsProvider credentialsProvider = getBuddyCredentials();
        AmazonSQS client = AmazonSQSClientBuilder.standard()
                .withRegion("us-west-2")
                .withCredentials(credentialsProvider)
                .build();

        // Assume that the principal is not able to send messages to arbitrary queues
        String queueUrl = sqs.createQueue(queueNamePrefix + "TestQueue").getQueueUrl();
        try {
            client.sendMessage(queueUrl, "Haxxors!!");
            assumeTrue("The buddy credentials should not authorize sending to arbitrary queues", false);
        } catch (AmazonSQSException e) {
            // Access Denied
            assumeThat(e.getStatusCode(), equalTo(403));
        } finally {
            sqs.deleteQueue(queueUrl);
        }

        return client;
    }

    protected Policy allowSendMessagePolicy(String roleARN) {
        Policy policy = new Policy();
        Statement statement = new Statement(Statement.Effect.Allow);
        statement.setActions(Collections.singletonList(SQSActions.SendMessage));
        statement.setPrincipals(new Principal(roleARN));
        statement.setResources(Collections.singletonList(new Resource("arn:aws:sqs:*:*:*")));
        policy.setStatements(Collections.singletonList(statement));
        return policy;
    }
}
