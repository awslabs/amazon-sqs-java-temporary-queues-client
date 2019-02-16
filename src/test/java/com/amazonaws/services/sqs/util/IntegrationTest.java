package com.amazonaws.services.sqs.util;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;

/**
 * Base class for integration tests
 */
public class IntegrationTest {
    
    protected AmazonSQS sqs;
    // UUIDs are too long for this
    protected String queueNamePrefix = "__" + getClass().getSimpleName() + "-" + ThreadLocalRandom.current().nextInt(1000000);
    
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
    }
}
