package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.executors.DeduplicatedRunnable.deduplicated;
import static com.amazonaws.services.sqs.executors.SerializableRunnable.serializable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.executors.SQSExecutorService;
import com.amazonaws.services.sqs.executors.SQSScheduledExecutorService;
import com.amazonaws.services.sqs.executors.SerializableReference;
import com.amazonaws.services.sqs.executors.SerializableRunnable;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import com.amazonaws.services.sqs.util.TestUtils;

public class SQSScheduledExecutorServiceIntegrationTest extends TestUtils {

    private static final Log LOG = LogFactory.getLog(SQSScheduledExecutorServiceIntegrationTest.class);
    
    private static AmazonSQS sqs;
    private static AmazonSQSResponsesClient sqsResponseClient;
    private static String queueUrl;
    private static List<SQSExecutorService> executors = new ArrayList<>();
    private static AtomicInteger seedCount = new AtomicInteger();
    private static CountDownLatch tasksCompletedLatch;

    private static class SQSScheduledExecutorWithAssertions extends SQSScheduledExecutorService implements Serializable {

        ConcurrentMap<String, Object> localScope = new ConcurrentHashMap<>();
        SerializableReference<SQSExecutorService> thisExecutor;

        public SQSScheduledExecutorWithAssertions( String queueUrl) {
            super(sqsResponseClient, sqsResponseClient, queueUrl);
            thisExecutor = new SerializableReference<>(queueUrl, this, localScope);
        }

        @Override
        protected SQSFutureTask<?> deserializeTask(Message message) {
            return thisExecutor.withScope(localScope, () -> super.deserializeTask(message));
        }

        protected Object writeReplace() throws ObjectStreamException {
            return thisExecutor;
        }
    }

    @Before
    public void setup() {
        sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();
        sqsResponseClient = new AmazonSQSResponsesClient(sqs);
        queueUrl = sqs.createQueue(generateRandomQueueName()).getQueueUrl();
        tasksCompletedLatch = new CountDownLatch(1);
        executors.clear();
    }

    @After
    public void teardown() throws InterruptedException {
        assertTrue(executors.parallelStream().allMatch(this::shutdownExecutor));
    }

    private boolean shutdownExecutor(SQSExecutorService executor) {
        try {
            executor.shutdown();
            return executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("Interrupted");
            return false;
        }
    }

    private SQSScheduledExecutorService createScheduledExecutor(String queueUrl) {
        SQSScheduledExecutorService executor = new SQSScheduledExecutorWithAssertions(queueUrl);
        executors.add(executor);
        return executor;
    }

    private static void seed(Executor executor) {
        seedCount.incrementAndGet();
        IntStream.range(0, 5)
                 .map(x -> x * 5)
                 .forEach(y -> {
                     executor.execute((SerializableRunnable)() -> sweepParent(executor, y)); 
                 });
    }

    private static void sweepParent(Executor executor, int number) {
        IntStream.range(number + 1, number + 5).forEach(x -> {
            executor.execute((SerializableRunnable)() -> sweepLeaf(executor, x));
        });
    }

    private static void sweepLeaf(Executor executor, int number) {
        if (tasksCompletedLatch.getCount() == 0) {
            throw new IllegalStateException("Too many leaves swept!");
        }
        tasksCompletedLatch.countDown();
    }

    @Test
    public void singleDelayedTask() throws InterruptedException {
        SQSScheduledExecutorService executor = createScheduledExecutor(queueUrl);
        executor.delayedExecute(serializable(() -> tasksCompletedLatch.countDown()), 1, TimeUnit.SECONDS);
        assertTrue(tasksCompletedLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void taskThatSpawnsTasksMultipleExecutors() throws InterruptedException {
        tasksCompletedLatch = new CountDownLatch(20);
        List<SQSScheduledExecutorService> sweepers = 
                IntStream.range(0, 5)
                         .mapToObj(x -> createScheduledExecutor(queueUrl))
                         .collect(Collectors.toList());
        sweepers.forEach(executor -> 
                executor.delayedExecute(deduplicated(() -> seed(executor)), 1, TimeUnit.SECONDS));
        assertTrue(tasksCompletedLatch.await(15, TimeUnit.SECONDS));
    }

    private static void slowTask() {
        LOG.info("Starting slow task");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        tasksCompletedLatch.countDown();
        LOG.info("Finished slow task");
    }

    @Test
    public void scheduleAtFixedRate() throws InterruptedException, ExecutionException {
        tasksCompletedLatch = new CountDownLatch(3);
        SQSScheduledExecutorService executor = createScheduledExecutor(queueUrl);
        Future<?> future = executor.scheduleAtFixedRate(serializable(SQSScheduledExecutorServiceIntegrationTest::slowTask), 1, 1, TimeUnit.SECONDS);
        assertTrue(tasksCompletedLatch.await(15, TimeUnit.SECONDS));
        assertFalse(future.isDone());
        
        // Cancel and assert that the future behaves correctly locally...
        future.cancel(true);
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        // TODO-RS: Switch to JUnit 5
        try {
            future.get();
            fail("Expected CancellationException");
        } catch (CancellationException e) {
            // Expected
        }
        
        // ...and that the message gets purged from the queue
        assertTrue(SQSQueueUtils.awaitEmptyQueue(sqs, queueUrl, 5, TimeUnit.SECONDS));
    }

    @Test
    public void scheduleWithFixedDelay() throws InterruptedException, ExecutionException {
        tasksCompletedLatch = new CountDownLatch(3);
        SQSScheduledExecutorService executor = createScheduledExecutor(queueUrl);
        Future<?> future = executor.scheduleAtFixedRate(serializable(SQSScheduledExecutorServiceIntegrationTest::slowTask), 1, 1, TimeUnit.SECONDS);
        assertTrue(tasksCompletedLatch.await(10, TimeUnit.SECONDS));
        assertFalse(future.isDone());
        
        // Cancel and assert that the future behaves correctly locally...
        future.cancel(true);
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        // TODO-RS: Switch to JUnit 5
        try {
            future.get();
            fail("Expected CancellationException");
        } catch (CancellationException e) {
            // Expected
        }
        
        // ...and that the message gets purged from the queue
        assertTrue(SQSQueueUtils.awaitEmptyQueue(sqs, queueUrl, 5, TimeUnit.SECONDS));
    }
}
