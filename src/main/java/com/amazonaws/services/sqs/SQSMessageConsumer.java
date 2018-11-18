package com.amazonaws.services.sqs;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

// TODO: AmazonSQSAsync support, in place of our own ExecutorService
public class SQSMessageConsumer {
	
	// TODO: Logging!
	// TODO: Cloudwatch metrics
	
	protected final AmazonSQS sqs;
	protected final String queueUrl;
	protected final Consumer<Message> consumer;
	protected final AtomicBoolean shuttingDown = new AtomicBoolean(false);
	
	// TODO: batch size, long polling, scale threads up and down...
	
	protected final ExecutorService executor;
	
	public SQSMessageConsumer(AmazonSQS sqs, String queueUrl, Consumer<Message> consumer) {
		this(sqs, queueUrl, Executors.newFixedThreadPool(1), consumer);
	}

	public SQSMessageConsumer(AmazonSQS sqs, String queueUrl, ExecutorService executor, Consumer<Message> consumer) {
		this.sqs = sqs;
		this.queueUrl = queueUrl;
		this.consumer = consumer;
		this.executor = executor;
	}
	
	public void start() {
		executor.execute(this::poll);
	}
	
	private void poll() {
		for (;;) {
			if (shuttingDown.get()) {
				break;
			}
			try {
				ReceiveMessageRequest request = new ReceiveMessageRequest()
						.withQueueUrl(queueUrl)
						.withWaitTimeSeconds(1)
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
				if ("Connection pool shut down".equals(e.getMessage())) {
					break;
				} else {
					e.printStackTrace();
				}
			} catch (Exception e) {
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
			e.printStackTrace();
			// TODO: This is only for better testing. We should be assuming temporary failure
			// by default (i.e. redrive the message) and only delete if we get a recognized
			// exception type that indicates otherwise.
//			sqs.changeMessageVisibility(queueUrl, message.getReceiptHandle(), 0);
			sqs.deleteMessage(queueUrl, message.getReceiptHandle());
		}
	}
	
	public void shutdown() {
		shuttingDown.set(true);
        executor.shutdown();
    }
	
	public boolean isShutdown() {
		return shuttingDown.get();
	}
	
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return executor.awaitTermination(timeout, unit);
	}
}
