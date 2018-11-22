package com.amazonaws.services.sqs.util;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

// TODO: AmazonSQSAsync support, in place of our own ExecutorService
public class SQSMessageConsumer {
	
	// TODO: Logging!
	
	protected final AmazonSQS sqs;
	protected final String queueUrl;
	protected final Consumer<Message> consumer;
	protected final AtomicBoolean shuttingDown = new AtomicBoolean(false);
	protected long deadlineNanos = -1;
	
	protected final ExecutorService executor;
	protected final Runnable shutdownHook;
	
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
	
	public void start(long timeout, TimeUnit unit) {
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
						.withMessageAttributeNames("All")
						.withAttributeNames("All");
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
					e.printStackTrace();
				}
			} catch (Exception e) {
			    // TODO-RS: Remove
				e.printStackTrace();
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
		    // TODO-RS: Logging
			e.printStackTrace();
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
