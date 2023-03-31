package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.util.IntegrationTest;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class IdleQueueSweeperIT extends IntegrationTest {

    private static AmazonSQSRequester requester;
    private static AmazonSQSResponder responder;
    private static String sweepingQueueUrl;
    private static IdleQueueSweeper sweeper;

    @BeforeEach
    public void setup() {
        requester = AmazonSQSRequesterClientBuilder.standard().withAmazonSQS(sqs).withIdleQueueSweepingPeriod(0, TimeUnit.SECONDS).build();
        responder = AmazonSQSResponderClientBuilder.standard().withAmazonSQS(sqs).build();
        sweepingQueueUrl = sqs.createQueue(queueNamePrefix).getQueueUrl();
        sweeper = new IdleQueueSweeper(requester, responder, sweepingQueueUrl, queueNamePrefix, 5, TimeUnit.SECONDS, exceptionHandler);
    }
    
    @AfterEach
    public void teardown() throws InterruptedException {
        if (sweeper != null) {
            sweeper.shutdown();
            sweeper.awaitTermination(30, TimeUnit.SECONDS);
        }
        if (sweepingQueueUrl != null) {
            sqs.deleteQueue(sweepingQueueUrl);
        }
        if (responder != null) {
            responder.shutdown();
        }
        if (requester != null) {
            requester.shutdown();
        }
    }
    
    @Test
    public void idleQueueSweeper() throws InterruptedException {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueNamePrefix + "-IdleQueue");
        String idleQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        sqs.tagQueue(idleQueueUrl, Collections.singletonMap(
                AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD_TAG, "1"));
        
        // May have to wait for up to a minute for the new queue to show up in ListQueues
        assertTrue(SQSQueueUtils.awaitQueueDeleted(sqs, idleQueueUrl, 70, TimeUnit.SECONDS),
                "Expected queue to be deleted: " + idleQueueUrl);
    }
}
