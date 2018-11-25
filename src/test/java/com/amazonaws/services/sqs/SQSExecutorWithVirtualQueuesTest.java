package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.executors.ExecutorUtils.applyIntOn;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.executors.SQSExecutorService;

public class SQSExecutorWithVirtualQueuesTest {

    private static AmazonSQS sqs;
    private static String requestQueueUrl;
    private static AmazonSQSResponsesClient rpcClient;
    private static List<SQSExecutorService> executors = new ArrayList<>();
    private static List<Throwable> taskExceptions = new ArrayList<>();

    @Before
    public void setup() {

        sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();
        // TODO-RS: Should be temporary queues in tests!
        requestQueueUrl = sqs.createQueue("RequestQueue-" + UUID.randomUUID().toString()).getQueueUrl();
        rpcClient = AmazonSQSResponsesClient.make(sqs, "SQSExecutorWithVirtualQueuesTest");
        executors.clear();
        taskExceptions.clear();
    }

    @After
    public void teardown() throws InterruptedException {
        if (rpcClient != null) {
            rpcClient.shutdown();
        }
    }

    @Test
    public void parallelMap() throws InterruptedException, ExecutionException, TimeoutException {
        SQSExecutorService executor = new SQSExecutorService(rpcClient, rpcClient, requestQueueUrl);
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
