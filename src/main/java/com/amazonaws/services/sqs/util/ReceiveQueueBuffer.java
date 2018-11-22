/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.sqs.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

/**
 * The ReceiveQueueBuffer class is responsible for dequeueing of messages from a single SQS queue.
 * <p>
 * Synchronization strategy: 
 * - Threads must hold the TaskSpawnSyncPoint object monitor to spawn a new task or modify the number of inflight tasks 
 * - Threads must hold the monitor of the "futures" list to modify the list 
 * - Threads must hold the monitor of the "finishedTasks" list to modify the list 
 * - If you need to lock both futures and finishedTasks, lock futures first and finishedTasks second
 */
public class ReceiveQueueBuffer {

    private static final Log LOG = LogFactory.getLog(ReceiveQueueBuffer.class);

    private static final Queue<Message> EMPTY_DEQUE = new ArrayDeque<Message>();
    
    private final ScheduledExecutorService waitTimer = Executors.newSingleThreadScheduledExecutor();
    
    private final AmazonSQS sqsClient;

    /**
     * This buffer's queue visibility timeout. Used to detect expired message that should not be
     * returned by the {@code receiveMessage} call.
     */
    private final long defaultVisibilityTimeoutNanos;

    /**
     * This buffer's queue default receive wait time. Used to set the timeout on futures so they complete
     * according to when the synchronous call to SQS would have.
     */
    private final long defaultWaitTimeNanos;
    
    /** shutdown buffer does not retrieve any more messages from sqs */
    volatile boolean shutDown = false;

    /** message delivery futures we gave out */
    private final Set<ReceiveMessageFuture> futures = new LinkedHashSet<>();

    /** finished batches are stored in this list. */
    protected LinkedList<ReceiveMessageBatchTask> finishedTasks = new LinkedList<>();

    public ReceiveQueueBuffer(AmazonSQS paramSQS, String queueUrl) {
        sqsClient = paramSQS;
        if (queueUrl.endsWith(".fifo")) {
            throw new IllegalArgumentException("FIFO queues are not yet supported: " + queueUrl);
        }
        
        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest().withQueueUrl(queueUrl)
                .withAttributeNames(QueueAttributeName.ReceiveMessageWaitTimeSeconds.toString(),
                					QueueAttributeName.VisibilityTimeout.toString());
        // TODO-RS: UserAgent?
        Map<String, String> attributes = sqsClient.getQueueAttributes(getQueueAttributesRequest).getAttributes();
		long visibilityTimeoutSeconds = Long.parseLong(attributes.get("VisibilityTimeout"));
        defaultVisibilityTimeoutNanos = TimeUnit.SECONDS.toNanos(visibilityTimeoutSeconds);
        long waitTimeSeconds = Long.parseLong(attributes.get("ReceiveMessageWaitTimeSeconds"));
        defaultWaitTimeNanos = TimeUnit.SECONDS.toNanos(waitTimeSeconds);
    }

    public ReceiveQueueBuffer(ReceiveQueueBuffer other) {
    	this.sqsClient = other.sqsClient;
    	this.defaultWaitTimeNanos = other.defaultWaitTimeNanos;
    	this.defaultVisibilityTimeoutNanos = other.defaultVisibilityTimeoutNanos;
    }

    /**
     * Prevents adding new futures and nacks all inflight messages.
     */
    public void shutdown() {
        shutDown = true;
        clear();
    }
    
    /**
     * Submits the request for retrieval of messages from the queue and returns a future that will
     * be signalled when the request is satisfied. The future may already be signalled by the time
     * it is returned.
     * 
     * @return never null
     */
    public Future<ReceiveMessageResult> receiveMessageAsync(ReceiveMessageRequest rq) {
        if (shutDown) {
            throw new AmazonClientException("The buffer has been shut down.");
        }

        // issue the future...
        int numMessages = 10;
        if (rq.getMaxNumberOfMessages() != null) {
            numMessages = rq.getMaxNumberOfMessages();
        }
        long waitTimeNanos;
        if (rq.getWaitTimeSeconds() != null) {
        	waitTimeNanos = TimeUnit.SECONDS.toNanos(rq.getWaitTimeSeconds());
        } else {
        	waitTimeNanos = defaultWaitTimeNanos;
        }
        ReceiveMessageFuture toReturn = issueFuture(numMessages, waitTimeNanos);

        // attempt to satisfy it right away...
        satisfyFuturesFromBuffer();
        
        toReturn.startWaitTimer();
        
        return toReturn;
    }

    /**
     * Creates and returns a new future object. Sleeps if the list of already-issued but as yet
     * unsatisfied futures is over a throttle limit.
     * 
     * @return never null
     */
    private ReceiveMessageFuture issueFuture(int size, Long waitTimeNanos) {
        synchronized (futures) {
            ReceiveMessageFuture theFuture = new ReceiveMessageFuture(size, waitTimeNanos);
            futures.add(theFuture);
            return theFuture;
        }
    }

    /**
     * Attempts to satisfy some or all of the already-issued futures from the local buffer. If the
     * buffer is empty or there are no futures, this method won't do anything.
     */
    protected void satisfyFuturesFromBuffer() {
        synchronized (futures) {
            synchronized (finishedTasks) {
                pruneExpiredFutures();
                
                // attempt to satisfy futures until we run out of either futures or
                // finished tasks
                Iterator<ReceiveMessageFuture> futureIter = futures.iterator(); 
                while (futureIter.hasNext() && (!finishedTasks.isEmpty())) {
                    // Remove any expired tasks before attempting to fufill the future
                    pruneExpiredTasks();
                    // Fufill the future from a non expired task if there is one. There is still a
                    // slight chance that the first task could have expired between the time we
                    // pruned and the time we fufill the future
                    if (!finishedTasks.isEmpty()) {
                        if (fufillFuture(futureIter.next())) {
                            futureIter.remove();
                        } else {
                            // We couldn't produce enough messages, so break the loop and return.
                            // We may not hit the while loop termination condition because we might
                            // have inflight FIFO messages that are blocking some of the messages.
                            return;
                        }
                    }
                }
            }
        }
    }

    /**
     * Fills the future with whatever results were received by the full batch currently at the head
     * of the completed batch queue. Those results may be retrieved messages, or an exception. This
     * method assumes that you are holding the finished tasks lock locks when invoking it. violate
     * this assumption at your own peril
     */
    private boolean fufillFuture(ReceiveMessageFuture future) {
        for (Iterator<ReceiveMessageBatchTask> iter = finishedTasks.iterator(); iter.hasNext();) {
            ReceiveMessageBatchTask task = iter.next();
            Exception exception = task.getException();
            if (exception != null) {
                // Only fulfill a future with an exception if it hasn't collected any messages yet!
                // Otherwise messages will be lost.
                if (future.messages.isEmpty()) {
                    iter.remove();
                    future.completeExceptionally(exception);
                    return true;
                }
            } else {
                task.populateResult(future);
                if (task.isEmpty()) {
                    task.clear();
                    iter.remove();
                }
                if (future.isFull()) {
                    return true;
                }
            }
        }
        
        if (!future.messages.isEmpty() || future.isExpired()) {
            future.complete();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Prune any expired tasks that do not have an exception associated with them. This method
     * assumes that you are holding the finishedTasks lock when invoking it
     */
    private void pruneExpiredTasks() {
        int numberExpiredTasksPruned = pruneHeadTasks(t -> t.isExpired() && t.getException() == null);

        // If we pruned any tasks because they are expired we also want to prune any empty tasks
        // afterwards so we have a chance to receive those expired messages again.
        if (numberExpiredTasksPruned > 0) {
            pruneHeadTasks(t -> t.isEmpty() && t.getException() == null);
        }
    }

    /**
     * Prune all tasks at the beginning of the finishedTasks list that meet the given condition.
     * Once a task is found that does not meet the given condition the pruning stops. This method
     * assumes that you are holding the finishedTasks lock when invoking it.
     * 
     * @param pruneCondition
     *            Condition on whether a task is eligible to be pruned
     * @return Number of total tasks pruned from finishedTasks
     */
    private int pruneHeadTasks(Predicate<ReceiveMessageBatchTask> pruneCondition) {
        int numberPruned = 0;
        while (!finishedTasks.isEmpty()) {
            ReceiveMessageBatchTask task = finishedTasks.getFirst();
            if (pruneCondition.test(task)) {
                task.clear();
                finishedTasks.removeFirst();
                numberPruned++;
            } else {
                break;
            }
        }
        return numberPruned;
    }

    private void pruneExpiredFutures() {
        for (Iterator<ReceiveMessageFuture> iterator = futures.iterator(); iterator.hasNext();) {
            ReceiveMessageFuture future = iterator.next();
            if (future.isExpired()) {
                future.complete();
                iterator.remove();
            }
        }
    }
    
    public void deliverMessages(List<Message> messages, String sourceQueueUrl, Integer visibilityTimeoutNanosOverride) {
    	submit(Runnable::run, () -> messages, sourceQueueUrl, visibilityTimeoutNanosOverride);
    }

    public void deliverException(Exception exception) {
    	submit(Runnable::run, () -> {throw exception;}, null, 0);
    }
    
    public void submit(Executor executor, Callable<List<Message>> callable, String queueUrl, Integer visibilityTimeoutSecondsOverride) {
        long visibilityTimeoutNanos;
    	if (visibilityTimeoutSecondsOverride == null) {
    		visibilityTimeoutNanos = defaultVisibilityTimeoutNanos;
    	} else {
    		visibilityTimeoutNanos = TimeUnit.SECONDS.toNanos(visibilityTimeoutSecondsOverride);
    	}
    	ReceiveMessageBatchTask task = new ReceiveMessageBatchTask(callable, queueUrl, visibilityTimeoutNanos);
        executor.execute(task);
    }
    
    /**
     * This method is called by the batches after they have finished retrieving the messages.
     */
    void reportBatchFinished(ReceiveMessageBatchTask batch) {
    	if (shutDown) {
    		batch.clear();
    		return;
    	}
    	
        synchronized (finishedTasks) {
            finishedTasks.addLast(batch);
        }
        satisfyFuturesFromBuffer();
    }

    /**
     * Clears and nacks any pre-fetched messages in this buffer.
     */
    public void clear() {
        boolean done = false;
        while (!done) {
            ReceiveMessageBatchTask currentBatch;
            synchronized (finishedTasks) {
                currentBatch = finishedTasks.poll();
            }

            if (currentBatch != null) {
                currentBatch.clear();
            } else {
                // ran out of batches to clear
                done = true;
            }
        }
    }

    protected class ReceiveMessageFuture extends CompletableFuture<ReceiveMessageResult> {
        /* how many messages did the request ask for */
        private final int requestedSize;

        private final List<Message> messages;
        
        private final Long waitTimeDeadlineNano;
        private Future<?> timeoutFuture;
        
        ReceiveMessageFuture(int paramSize, Long waitTimeNanos) {
            requestedSize = paramSize;
            messages = new ArrayList<>(requestedSize);
            
            if (waitTimeNanos != null) {
                this.waitTimeDeadlineNano = System.nanoTime() + waitTimeNanos;
            } else {
                this.waitTimeDeadlineNano = null;
            }
            
            whenComplete((result, exception) -> cancelTimeout());
        }

        public synchronized void startWaitTimer() {
            if (waitTimeDeadlineNano == null || isDone() || timeoutFuture != null) {
                return;
            }
            
            long remaining = waitTimeDeadlineNano - System.nanoTime();
            if (remaining < 0) {
                timeout();
            } else {
                timeoutFuture = waitTimer.schedule(this::timeout, remaining, TimeUnit.NANOSECONDS);
            }
        }
        
        public boolean isExpired() {
            return waitTimeDeadlineNano != null && System.nanoTime() > waitTimeDeadlineNano;
        }
        
        public synchronized void addMessage(Message message) {
            if (isDone()) {
                throw new IllegalStateException("Future is already completed");
            }
            if (isFull()) {
                throw new IllegalStateException("Future already has enough messages");
            }
            messages.add(message);
            if (isFull()) {
                complete();
            }
        }
        
        public boolean isFull() {
            return messages.size() >= requestedSize;
        }
        
        public synchronized void timeout() {
            if (!isDone()) {
                complete();
            }
        }
        
        public synchronized void complete() {
            if (!isDone()) {
                ReceiveMessageResult result = new ReceiveMessageResult();
                result.setMessages(messages);
                complete(result);
            }
        }
        
        private synchronized void cancelTimeout() {
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
        }
    }

    /**
     * Task to receive messages from SQS.
     * <p>
     * The batch task is constructed {@code !open} until the {@code ReceiveMessage} completes. At
     * that point, the batch opens and its messages (if any) become available to read.
     */
    protected class ReceiveMessageBatchTask extends FutureTask<List<Message>> {
    	private Exception exception = null;
        protected Queue<Message> messages;
        private final String sourceQueueUrl;
        private final long visibilityTimeoutNanos;
        private long visibilityDeadlineNano;
        private Future<?> expiryFuture;

        /**
         * Constructs a receive task waiting the specified time before calling SQS.
         * 
         * @param waitTimeMs
         *            the time to wait before calling SQS
         */
        ReceiveMessageBatchTask(Callable<List<Message>> callable, String sourceQueueUrl, long visibilityTimeoutNanos) {
        	super(callable);
        	this.sourceQueueUrl = sourceQueueUrl;
        	this.visibilityTimeoutNanos = visibilityTimeoutNanos;
            messages = EMPTY_DEQUE;
        }
        
        synchronized boolean isEmpty() {
        	if (!isDone()) {
        		throw new IllegalStateException();
        	}
            return messages.isEmpty();
        }

        /**
         * @return the exception that was thrown during execution, or null if there was no exception
         */
        synchronized Exception getException() {
        	if (!isDone()) {
        		throw new IllegalStateException();
        	}
        	return exception;
        }


        synchronized void populateResult(ReceiveMessageFuture future) {
            if (!isDone()) {
                throw new IllegalStateException("batch is not open");
            }

            // our messages expired.
            if (isExpired()) {
                clear();
                return;
            }

            if (messages.isEmpty()) {
                return;
            }
            
            for (Iterator<Message> iter = messages.iterator(); iter.hasNext() && !future.isFull();) {
                Message message = iter.next();
                iter.remove();
                future.addMessage(message);
            }
        }
        
        public synchronized void startExpiryTimer() {
            if (isExpired() || expiryFuture != null) {
                return;
            }
            
            long remaining = visibilityDeadlineNano - System.nanoTime();
            if (remaining < 0) {
                clear();
            } else {
                expiryFuture = waitTimer.schedule(this::clear, remaining, TimeUnit.NANOSECONDS);
            }
        }
        
        boolean isExpired() {
            return System.nanoTime() > visibilityDeadlineNano;
        }

        /**
         * Nacks and clears all messages remaining in the batch.
         */
        synchronized void clear() {
            if (!isDone()) {
                throw new IllegalStateException("batch is not open");
            }

            if (expiryFuture != null) {
                expiryFuture.cancel(false);
            }
            
            if (!isExpired()) {
                nackMessages(messages);
            }
            messages.clear();
        }

        protected void nackMessages(Collection<Message> messages) {
            if (messages.isEmpty()) {
                return;
            }
            
            ChangeMessageVisibilityBatchRequest batchRequest = new ChangeMessageVisibilityBatchRequest().withQueueUrl(sourceQueueUrl);
            // TODO-RS: UserAgent?

            List<ChangeMessageVisibilityBatchRequestEntry> entries = 
                    new ArrayList<ChangeMessageVisibilityBatchRequestEntry>(messages.size());

            int i = 0;
            for (Message m : messages) {

                entries.add(new ChangeMessageVisibilityBatchRequestEntry().withId(Integer.toString(i))
                        .withReceiptHandle(m.getReceiptHandle()).withVisibilityTimeout(0));
                ++i;
            }

            try {
                batchRequest.setEntries(entries);
                sqsClient.changeMessageVisibilityBatch(batchRequest);
            } catch (AmazonClientException e) {
                // Log and ignore.
                LOG.warn("ReceiveMessageBatchTask: changeMessageVisibility failed " + e);
            }
        }
        
        @Override
        protected void set(List<Message> v) {
        	messages = new ArrayDeque<Message>(v);
        	visibilityDeadlineNano = System.nanoTime() + visibilityTimeoutNanos;
        	super.set(v);
        }
        
        @Override
        protected void done() {
        	reportBatchFinished(this);
        }
    }
}