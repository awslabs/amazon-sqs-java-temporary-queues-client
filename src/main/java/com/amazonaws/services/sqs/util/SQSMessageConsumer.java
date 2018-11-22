package com.amazonaws.services.sqs.util;

import static com.amazonaws.services.sqs.util.SQSQueueUtils.ATTRIBUTE_NAMES_ALL;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
public class SQSMessageConsumer {

    private static final Log LOG = LogFactory.getLog(ReceiveQueueBuffer.class);

    protected final AmazonSQS sqs;
    protected final String queueUrl;
    protected final Consumer<Message> consumer;

    protected final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    protected final Runnable shutdownHook;
    // TODO-RS: This is currently only defining a soft deadline for shutdown:
    // it only limits the WaitTimeSeconds parameter, but will still wait for
    // delayed completion of inflight receives.
    protected long deadlineNanos = -1;

    // TODO: AmazonSQSAsync support, in place of our own ExecutorService
    protected final ExecutorService executor;

    public SQSMessageConsumer(AmazonSQS sqs, String queueUrl, Consumer<Message> consumer) {
        this(sqs, queueUrl, consumer, () -> {});
    }

    public SQSMessageConsumer(AmazonSQS sqs, String queueUrl, Consumer<Message> consumer, Runnable shutdownHook) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.consumer = consumer;
        this.executor = Executors.newFixedThreadPool(1);
        this.shutdownHook = shutdownHook;
    }

    public void start() {
        executor.execute(this::poll);
    }

    public void runFor(long timeout, TimeUnit unit) {
        deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        start();
    }

    private void poll() {
        for (;;) {
            if (shuttingDown.get()) {
                break;
            }

            int waitTimeSeconds = 20;
            if (deadlineNanos > 0) {
                long currentNanos = System.nanoTime();
                if (currentNanos >= deadlineNanos) {
                    shutdown();
                    break;
                } else {
                    int secondsRemaining = (int)TimeUnit.NANOSECONDS.toSeconds(deadlineNanos - currentNanos);
                    waitTimeSeconds = Math.max(0, Math.min(20, secondsRemaining));
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

                messages.parallelStream().forEach(this::accept);
            } catch (QueueDoesNotExistException e) {
                // Ignore, it may be recreated!
                // Slow down on the polling though, to avoid tight looping.
                // This can be treated similar to an empty queue.
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            } catch (IllegalStateException e) {
                // TODO-RS: This is a hack
                if ("Connection pool shut down".equals(e.getMessage())) {
                    break;
                } else {
                    LOG.error("Unexpected exception", e);
                }
            } catch (Exception e) {
                LOG.error("Unexpected exception", e);
            }
        }
    }

    private void accept(Message message) {
        if (shuttingDown.get()) {
            sqs.changeMessageVisibility(queueUrl, message.getReceiptHandle(), 0);
            return;
        }
        try {
            consumer.accept(message);
            sqs.deleteMessage(queueUrl, message.getReceiptHandle());
        } catch (QueueDoesNotExistException e) {
            // Ignore
        } catch (RuntimeException e) {
            LOG.error("Exception encounted while processing message with ID " + message.getMessageId(), e);
            sqs.changeMessageVisibility(queueUrl, message.getReceiptHandle(), 0);
        }
    }

    public void shutdown() {
        shuttingDown.set(true);
        executor.shutdown();
        shutdownHook.run();
    }

    public boolean isShutdown() {
        return shuttingDown.get();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }
}
