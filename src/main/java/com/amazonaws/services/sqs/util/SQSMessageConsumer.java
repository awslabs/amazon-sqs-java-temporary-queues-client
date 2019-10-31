package com.amazonaws.services.sqs.util;

import static com.amazonaws.services.sqs.util.SQSQueueUtils.ATTRIBUTE_NAMES_ALL;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

/**
 * A very basic utility class for continuously polling for messages from an
 * Amazon SQS queue. It uses a single thread to receive up to 10 messages at once
 * and dispatch in parallel to a given {@code Consumer<Message>} callback.
 * <p>
 * This class will be augmented in the future to support a great deal more
 * configuration and sophistication around scaling up and down to meet
 * demand from the queue.
 */
public class SQSMessageConsumer implements AutoCloseable {

    protected final AmazonSQS sqs;
    protected final String queueUrl;
    protected final Consumer<Message> consumer;
    protected final Consumer<Exception> exceptionHandler;
    
    protected final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    protected final CountDownLatch terminated = new CountDownLatch(1);
    protected final Runnable shutdownHook;
    // TODO-RS: This is currently only defining a soft deadline for shutdown:
    // it only limits the WaitTimeSeconds parameter, but will still wait for
    // delayed completion of inflight receives. We do want to allow those to
    // complete so they can be put back on the queue, but isShutdown() should
    // be true in the meantime.
    protected long deadlineNanos = -1;

    protected int maxWaitTimeSeconds;
    
    protected final int pollingThreadCount;

    private static final ExecutorService executor = Executors.newCachedThreadPool(
            new DaemonThreadFactory(SQSMessageConsumer.class.getSimpleName()));

    /**
     * @deprecated Please use {@link SQSMessageConsumerBuilder} instead.
     */
    @Deprecated
    public SQSMessageConsumer(AmazonSQS sqs, String queueUrl, Consumer<Message> consumer) {
        this(sqs, queueUrl, consumer, () -> {}, SQSQueueUtils.DEFAULT_EXCEPTION_HANDLER);
    }

    /**
     * @deprecated Please use {@link SQSMessageConsumerBuilder} instead.
     */
    @Deprecated
    public SQSMessageConsumer(AmazonSQS sqs, String queueUrl, Consumer<Message> consumer,
                              Runnable shutdownHook, Consumer<Exception> exceptionHandler) {
        this(sqs, queueUrl, consumer, () -> {}, SQSQueueUtils.DEFAULT_EXCEPTION_HANDLER, 20, 1);
    }
    
    SQSMessageConsumer(AmazonSQS sqs, String queueUrl, Consumer<Message> consumer,
                       Runnable shutdownHook, Consumer<Exception> exceptionHandler,
                       int maxWaitTimeSeconds, int pollingThreadCount) {
        this.sqs = Objects.requireNonNull(sqs);
        this.queueUrl = Objects.requireNonNull(queueUrl);
        this.consumer = Objects.requireNonNull(consumer);
        this.shutdownHook = Objects.requireNonNull(shutdownHook);
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
        this.maxWaitTimeSeconds = maxWaitTimeSeconds;
        this.pollingThreadCount = pollingThreadCount;
    }

    public void start() {
        for ( int i = 0; i < this.pollingThreadCount; i++ ) {
            executor.execute(this::poll);
        }
    }

    public void runFor(long timeout, TimeUnit unit) {
        deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        start();
    }

    private void poll() {
        try {
            for (;;) {
                if (Thread.interrupted() || shuttingDown.get()) {
                    break;
                }

                int waitTimeSeconds = maxWaitTimeSeconds;
                if (deadlineNanos > 0) {
                    long currentNanos = System.nanoTime();
                    if (currentNanos >= deadlineNanos) {
                        shutdown();
                        break;
                    } else {
                        int secondsRemaining = (int)TimeUnit.NANOSECONDS.toSeconds(deadlineNanos - currentNanos);
                        waitTimeSeconds = Math.max(0, Math.min(maxWaitTimeSeconds, secondsRemaining));
                    }
                }
    
                try {
                    ReceiveMessageRequest request = new ReceiveMessageRequest()
                            .withQueueUrl(queueUrl)
                            .withWaitTimeSeconds(waitTimeSeconds)
                            .withMaxNumberOfMessages(10)
                            .withMessageAttributeNames(ATTRIBUTE_NAMES_ALL)
                            .withAttributeNames(ATTRIBUTE_NAMES_ALL);
                    List<Message> messages = sqs.receiveMessage(request).getMessages();
    
                    messages.parallelStream().forEach(this::handleMessage);
                } catch (QueueDoesNotExistException e) {
                    // Ignore, it may be recreated!
                    // Slow down on the polling though, to avoid tight looping.
                    // This can be treated similar to an empty queue.
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                } catch (Exception e) {
                    exceptionHandler.accept(e);
                }
            }
        } finally {
            terminated.countDown();
        }
    }

    private void handleMessage(Message message) {
        if (shuttingDown.get()) {
            sqs.changeMessageVisibility(queueUrl, message.getReceiptHandle(), 0);
            return;
        }
        try {
            accept(message);
            sqs.deleteMessage(queueUrl, message.getReceiptHandle());
        } catch (QueueDoesNotExistException e) {
            // Ignore
        } catch (RuntimeException processingException) {
            // TODO-RS: separate accept from delete in exception handling
            String errorMessage = "Exception encountered while processing message with ID " + message.getMessageId();
            exceptionHandler.accept(new RuntimeException(errorMessage, processingException));
            
            try {
                sqs.changeMessageVisibility(queueUrl, message.getReceiptHandle(), 0);
            } catch (QueueDoesNotExistException e) {
                // Ignore
            } catch (RuntimeException cmvException) {
                String cmvErrorMessage = "Exception encountered while changing message visibility with ID " + message.getMessageId();
                exceptionHandler.accept(new RuntimeException(cmvErrorMessage, cmvException));
            }
        }
    }

    protected void accept(Message message) {
        consumer.accept(message);
    }
    
    public void shutdown() {
        if (shuttingDown.compareAndSet(false, true)) {
            runShutdownHook();
        }
    }

    protected void runShutdownHook() {
        shutdownHook.run();
    }
    
    public boolean isShutdown() {
        return shuttingDown.get();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminated.await(timeout, unit);
    }

    public void terminate() {
        shutdown();
        try {
            awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    @Override
    public void close() {
        shutdown();
    }
}
