package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.executors.ExecutorUtils.applyIntOn;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.executors.SQSExecutorService;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;

public class SQSExecutorWithVirtualQueuesTest {

	private static AmazonSQS sqs;
	private static String requestQueueUrl;
    private static AmazonSQSWithResponses rpcClient;
	private static List<SQSExecutorService> executors = new ArrayList<>();
    private static AtomicInteger seedCount = new AtomicInteger();
    private static CountDownLatch tasksCompletedLatch;
    private static List<Throwable> taskExceptions = new ArrayList<>();
    
    @Before
    public void setup() {

//        final AWSCredentialsProvider credentialsProvider = new OdinAWSCredentialsProvider(
//            TestUtils.getOdinMaterialSet());
        
        sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
//                .withCredentials(credentialsProvider)
                .build();
        // TODO-RS: Should be temporary queues in tests!
        requestQueueUrl = sqs.createQueue("RequestQueue-" + UUID.randomUUID().toString()).getQueueUrl();
        rpcClient = AmazonSQSResponsesClient.make(sqs, "SQSExecutorWithVirtualQueuesTest");
        tasksCompletedLatch = new CountDownLatch(1);
        executors.clear();
        taskExceptions.clear();
    }
    
    @After
    public void teardown() throws InterruptedException {
        rpcClient.shutdown();
    }
    
    private static void seed(Executor executor) {
        seedCount.incrementAndGet();
        IntStream.range(0, 5)
                 .map(x -> x * 5)
                 .forEach(y -> {
                	 executor.execute((Serializable & Runnable)() -> sweepParent(executor, y)); 
                 });
    }
    
    private static void sweepParent(Executor executor, int number) {
    	IntStream.range(number + 1, number + 5).forEach(x -> {
    		executor.execute((Serializable & Runnable)() -> sweepLeaf(executor, x));
    	});
    }
    
    private static void sweepLeaf(Executor executor, int number) {
    	if (tasksCompletedLatch.getCount() == 0) {
    		throw new IllegalStateException("Too many leaves swept!");
    	}
    	tasksCompletedLatch.countDown();
    }
    
    @Test
    public void parallelMap() throws InterruptedException, ExecutionException, TimeoutException {
        SQSExecutorService executor = new SQSExecutorService(rpcClient, requestQueueUrl, null);
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
