package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.executors.ExecutorUtils.applyIntOn;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.util.IntegrationTest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;

public class SQSExecutorWithVirtualQueuesIT extends IntegrationTest {

    private AmazonSQSRequester requester;
    private AmazonSQSResponder responder;
    private String requestQueueUrl;
    private SQSExecutorService executor;

    @Before
    public void setup() {
        requester = AmazonSQSRequesterClientBuilder.standard().withAmazonSQS(sqs).withInternalQueuePrefix(queueNamePrefix).build();
        responder = AmazonSQSResponderClientBuilder.standard().withAmazonSQS(sqs).build();
        requestQueueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(queueNamePrefix + "-RequestQueue").build()).queueUrl();
        executor = new SQSExecutorService(requester, responder, requestQueueUrl, exceptionHandler);
    }

    @After
    public void teardown() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        responder.shutdown();
        requester.shutdown();
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(requestQueueUrl).build());
    }

    @Test
    public void parallelMap() throws InterruptedException, ExecutionException, TimeoutException {
        int sum = IntStream.range(0, 10)
                           .parallel()
                           .mapToObj(applyIntOn(executor, i -> i * i))
                           .mapToInt(Integer::intValue)
                           .sum();
        int expected = IntStream.range(0, 10)
                                .mapToObj(i -> i * i)
                                .mapToInt(Integer::intValue)
                                .sum();
        assertEquals(expected, sum);
    }
}
