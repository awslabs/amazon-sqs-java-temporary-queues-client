package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.executors.SerializableReference;
import com.amazonaws.services.sqs.executors.SerializableRunnable;
import com.amazonaws.services.sqs.util.IntegrationTest;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.amazonaws.services.sqs.executors.DeduplicatedRunnable.deduplicated;
import static com.amazonaws.services.sqs.executors.SerializableRunnable.serializable;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SQSScheduledExecutorServiceIT extends IntegrationTest {

    private static AmazonSQSRequester requester;
    private static AmazonSQSResponder responder;
    private static String queueUrl;
    private static final List<SQSExecutorService> executors = new ArrayList<>();
    private static final AtomicInteger seedCount = new AtomicInteger();
    private static AtomicInteger tasksRemaining;

    private static class SQSScheduledExecutorWithAssertions extends SQSScheduledExecutorService implements Serializable {

        SerializableReference<SQSExecutorService> thisExecutor;

        public SQSScheduledExecutorWithAssertions(String queueUrl, Consumer<Exception> exceptionHandler) {
            super(requester, responder, queueUrl, exceptionHandler);
            thisExecutor = new SerializableReference<>(queueUrl, this);
        }

        @Override
        protected SQSFutureTask<?> deserializeTask(Message message) {
            return thisExecutor.withScope(() -> super.deserializeTask(message));
        }

        protected Object writeReplace() throws ObjectStreamException {
            return thisExecutor.proxy();
        }
    }

    @BeforeEach
    public void setup() {
        requester = new AmazonSQSRequesterClient(sqs, queueNamePrefix,
                Collections.emptyMap(), exceptionHandler);
        responder = new AmazonSQSResponderClient(sqs);
        queueUrl = sqs.createQueue(CreateQueueRequest.builder().queueName(queueNamePrefix + "-RequestQueue").build()).queueUrl();
        tasksRemaining = new AtomicInteger(1);
        executors.clear();
    }

    private void awaitTasksSeconds(int minimumSeconds, int maximumSeconds) {
        await().atLeast(minimumSeconds, TimeUnit.SECONDS).and()
               .atMost(maximumSeconds, TimeUnit.SECONDS)
               .untilAtomic(tasksRemaining, equalTo(0));
    }

    @AfterEach
    public void teardown() {
        assertTrue(executors.parallelStream().allMatch(this::shutdownExecutor));
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        responder.shutdown();
        requester.shutdown();
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
        SQSScheduledExecutorService executor = new SQSScheduledExecutorWithAssertions(queueUrl, exceptionHandler);
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
        if (tasksRemaining.get() == 0) {
            throw new IllegalStateException("Too many leaves swept!");
        }
        tasksRemaining.decrementAndGet();
    }

    @Test
    public void singleDelayedTask() throws InterruptedException {
        SQSScheduledExecutorService executor = createScheduledExecutor(queueUrl);
        executor.delayedExecute(serializable(() -> tasksRemaining.decrementAndGet()), 1, TimeUnit.SECONDS);
        awaitTasksSeconds(1, 5);
    }

    @Test
    public void taskThatSpawnsTasksMultipleExecutors() throws InterruptedException {
        tasksRemaining = new AtomicInteger(20);
        List<SQSScheduledExecutorService> sweepers = 
                IntStream.range(0, 5)
                         .mapToObj(x -> createScheduledExecutor(queueUrl))
                         .collect(Collectors.toList());
        sweepers.forEach(executor -> 
                executor.delayedExecute(deduplicated(() -> seed(executor)), 1, TimeUnit.SECONDS));
        awaitTasksSeconds(1, 15);
    }

    private static void slowTask() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        tasksRemaining.decrementAndGet();
    }

    private static void fastTask() {
        tasksRemaining.decrementAndGet();
    }

    @Test
    public void scheduleSlowTaskAtFixedRate() throws InterruptedException, ExecutionException {
        tasksRemaining = new AtomicInteger(3);
        SQSScheduledExecutorService executor = createScheduledExecutor(queueUrl);
        Future<?> future = executor.scheduleAtFixedRate(serializable(SQSScheduledExecutorServiceIT::slowTask), 1, 1, TimeUnit.SECONDS);
        awaitTasksSeconds(3, 15);
        assertScheduledTaskCanBeCancelled(future);
    }

    @Test
    public void scheduleSlowTaskWithFixedDelay() throws InterruptedException, ExecutionException {
        tasksRemaining = new AtomicInteger(3);
        SQSScheduledExecutorService executor = createScheduledExecutor(queueUrl);
        Future<?> future = executor.scheduleWithFixedDelay(serializable(SQSScheduledExecutorServiceIT::slowTask), 1, 1, TimeUnit.SECONDS);
        awaitTasksSeconds(9, 15);
        assertScheduledTaskCanBeCancelled(future);
    }

    @Test
    public void scheduleFastTaskAtFixedRate() throws InterruptedException, ExecutionException {
        tasksRemaining = new AtomicInteger(3);
        SQSScheduledExecutorService executor = createScheduledExecutor(queueUrl);
        Future<?> future = executor.scheduleAtFixedRate(serializable(SQSScheduledExecutorServiceIT::fastTask), 1, 1, TimeUnit.SECONDS);
        awaitTasksSeconds(3, 15);
        assertScheduledTaskCanBeCancelled(future);
    }

    @Test
    public void scheduleFastTaskWithFixedDelay() throws InterruptedException, ExecutionException {
        tasksRemaining = new AtomicInteger(3);
        SQSScheduledExecutorService executor = createScheduledExecutor(queueUrl);
        Future<?> future = executor.scheduleWithFixedDelay(serializable(SQSScheduledExecutorServiceIT::fastTask), 1, 1, TimeUnit.SECONDS);
        awaitTasksSeconds(3, 15);
        assertScheduledTaskCanBeCancelled(future);
    }

    public void assertScheduledTaskCanBeCancelled(Future<?> future) throws ExecutionException, InterruptedException {
        assertFalse(future.isDone());

        // Cancel and assert that the future behaves correctly locally...
        future.cancel(true);
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        assertThrows(CancellationException.class, future::get);

        // ...and that the message gets purged from the queue
        assertTrue(SQSQueueUtils.awaitEmptyQueue(sqs, queueUrl, 10, TimeUnit.SECONDS));
    }
}
