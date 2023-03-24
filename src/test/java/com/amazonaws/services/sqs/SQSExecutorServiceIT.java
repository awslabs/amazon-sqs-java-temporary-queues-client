package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.executors.SerializableCallable;
import com.amazonaws.services.sqs.executors.SerializableReference;
import com.amazonaws.services.sqs.executors.SerializableRunnable;
import com.amazonaws.services.sqs.util.IntegrationTest;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.amazonaws.services.sqs.executors.DeduplicatedCallable.deduplicated;
import static com.amazonaws.services.sqs.executors.ExecutorUtils.applyIntOn;
import static com.amazonaws.services.sqs.executors.SerializableCallable.serializable;
import static com.amazonaws.services.sqs.executors.SerializableRunnable.serializable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SQSExecutorServiceIT extends IntegrationTest {

    private static AmazonSQSRequester requester;
    private static AmazonSQSResponder responder;
    private static String queueUrl;
    private static final List<SQSExecutorService> executors = new ArrayList<>();
    private static final AtomicInteger seedCount = new AtomicInteger();
    private static CountDownLatch tasksCompletedLatch;

    private static class SQSExecutorWithAssertions extends SQSExecutorService implements Serializable {

        SerializableReference<SQSExecutorService> thisExecutor;

        public SQSExecutorWithAssertions(String queueUrl, Consumer<Exception> exceptionHandler) {
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
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueNamePrefix + "-RequestQueue").build();
        queueUrl = sqs.createQueue(createQueueRequest).queueUrl();
        tasksCompletedLatch = new CountDownLatch(1);
        executors.clear();
    }

    @AfterEach
    public void teardown() {
        try {
            assertTrue(executors.parallelStream().allMatch(this::shutdownExecutor));
        } finally {
            requester.shutdown();
            responder.shutdown();
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        }
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

    private SQSExecutorService createExecutor(String queueUrl) {
        SQSExecutorService executor = new SQSExecutorWithAssertions(queueUrl, exceptionHandler);
        executors.add(executor);
        return executor;
    }

    private static void seed(Executor executor) {
        seedCount.incrementAndGet();
        IntStream.range(0, 5)
                 .map(x -> x * 5)
                 .forEach(y -> executor.execute((SerializableRunnable)() -> sweepParent(executor, y)));
    }

    private static void sweepParent(Executor executor, int number) {
        IntStream.range(number + 1, number + 5).forEach(x -> executor.execute((SerializableRunnable)() -> sweepLeaf(executor, x)));
    }

    private static void sweepLeaf(Executor executor, int number) {
        if (tasksCompletedLatch.getCount() == 0) {
            throw new IllegalStateException("Too many leaves swept!");
        }
        tasksCompletedLatch.countDown();
    }

    @Test
    public void singleExecute() throws InterruptedException {
        SQSExecutorService executor = createExecutor(queueUrl);
        executor.execute(serializable(() -> tasksCompletedLatch.countDown()));
        assertTrue(tasksCompletedLatch.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void singleSubmitRunnable() throws InterruptedException, ExecutionException, TimeoutException {
        SQSExecutorService executor = createExecutor(queueUrl);
        Future<?> future = executor.submit(serializable(() -> sweepLeaf(executor, 42)));
        assertNull(future.get(2, TimeUnit.SECONDS));
        future.cancel(true);
    }

    @Test
    public void singleSubmitCallable() throws InterruptedException, ExecutionException, TimeoutException {
        SQSExecutorService executor = createExecutor(queueUrl);
        Future<Integer> future = executor.submit(serializable(() -> 2 + 2));
        assertEquals(4, future.get(2, TimeUnit.SECONDS).intValue());
        future.cancel(true);
    }

    private SerializableCallable<Integer> squareTask(int i) {
        return () -> i * i;
    }

    private <T> T safeFutureGet(Future<T> future, long timeout, TimeUnit unit) {
        try {
            return future.get(timeout, unit);
        } catch (ExecutionException e) {
            throw (RuntimeException)e.getCause();
        } catch (TimeoutException|InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            future.cancel(true);
        }
    }

    @Test
    public void invokeAll() throws InterruptedException, ExecutionException, TimeoutException {
        SQSExecutorService executor = createExecutor(queueUrl);
        List<Callable<Integer>> tasks = IntStream.range(0, 10)
                .mapToObj(this::squareTask)
                .collect(Collectors.toList());

        int sum = executor.invokeAll(tasks)
                          .stream()
                          .map(x -> safeFutureGet(x, 5, TimeUnit.SECONDS))
                          .mapToInt(Integer::intValue)
                          .sum();
        int expected = IntStream.range(0, 10)
                                .mapToObj(i -> i * i)
                                .mapToInt(Integer::intValue)
                                .sum();
        assertEquals(expected, sum);
    }

    @Test
    public void taskThatSpawnsTasksLocal() throws InterruptedException {
        // Sanity test using a local executor service
        tasksCompletedLatch = new CountDownLatch(20);
        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(() -> seed(executor));
        assertTrue(tasksCompletedLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void taskThatSpawnsTasks() throws InterruptedException {
        tasksCompletedLatch = new CountDownLatch(20);

        SQSExecutorService executor = createExecutor(queueUrl);
        executor.execute(() -> seed(executor));
        assertTrue(tasksCompletedLatch.await(1, TimeUnit.MINUTES));
    }

    @Test
    public void parallelMapLocal() {
        // Sanity test using a local executor service
        Set<Integer> actual = IntStream.range(0, 10)
                                       .parallel()
                                       .map(i -> i * i)
                                       .boxed()
                                       .collect(Collectors.toSet());
        Set<Integer> expected = new HashSet<>(Arrays.asList(0,1,4,9,16,25,36,49,64,81));
        assertEquals(expected, actual);
    }

    @Test
    public void parallelMap() {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Set<Integer> actual = IntStream.range(0, 10)
                                       .mapToObj(applyIntOn(executor, i -> i * i))
                                       .collect(Collectors.toSet());
        Set<Integer> expected = new HashSet<>(Arrays.asList(0,1,4,9,16,25,36,49,64,81));
        assertEquals(expected, actual);
    }

    private static Function<String, List<String>> listQueuesFunction(SerializableReference<SqsClient> sqsRef) {
        return (Function<String, List<String>> & Serializable)p -> sqsRef.get().listQueues(ListQueuesRequest.builder().queueNamePrefix(p).build()).queueUrls();
    }
    
    @Test
    public void listQueuesLimitWorkaround() throws InterruptedException {
        String prefix = queueNamePrefix + "-listQueuesLimitWorkaround-";
        int numQueues = 50;
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < numQueues; i++) {
            String queueName = prefix + i;
            expected.add(sqs.createQueue(CreateQueueRequest.builder().queueName(queueName).build()).queueUrl());
        }
        try {
            SQSQueueUtils.awaitWithPolling(5, 70, TimeUnit.SECONDS, () -> sqs.listQueues(ListQueuesRequest.builder().queueNamePrefix(prefix).build()).queueUrls().size() == numQueues);
            List<SQSExecutorService> executors = 
                    IntStream.range(0, 5)
                             .mapToObj(x -> createExecutor(queueUrl))
                             .collect(Collectors.toList());
            try (SerializableReference<SqsClient> sqsRef = new SerializableReference<>("SQS", sqs, true)) {
                Function<String, List<String>> lister = SQSExecutorServiceIT.listQueuesFunction(sqsRef);
                Set<String> allQueueUrls = new HashSet<>(SQSQueueUtils.listQueues(executors.get(0), lister, prefix, 50));
                assertEquals(expected.size(), allQueueUrls.size());
                assertEquals(expected, allQueueUrls);
            }
        } finally {
            expected.parallelStream().forEach(queueUrl -> {
                try {
                    sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
                } catch (QueueDoesNotExistException e) {
                    // Ignore
                }
            });
        }
    }

    @Test
    public void dedupedSubmit() throws InterruptedException, ExecutionException, TimeoutException {
        SQSExecutorService executor = createExecutor(queueUrl);
        Future<Integer> future = executor.submit(deduplicated(() -> 2 + 2));
        assertEquals(Integer.valueOf(4), future.get(2, TimeUnit.SECONDS));

        future = executor.submit(deduplicated(() -> 2 + 2));
        assertEquals(Integer.valueOf(4), future.get(2, TimeUnit.SECONDS));
    }

    @Test
    public void deduplicationOnSending() {
        // TODO-RS
    }

    @Test
    public void deduplicationOnReceiving() {
        // TODO-RS
    }

    @Test
    public void deduplicationOnSendingResponse() {
        // TODO-RS
    }

    @Test
    public void deserializeTaskError() {
        // TODO-RS
    }
}
