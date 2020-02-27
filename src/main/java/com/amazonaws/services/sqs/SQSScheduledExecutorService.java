package com.amazonaws.services.sqs;

import static com.amazonaws.services.sqs.util.SQSQueueUtils.getLongMessageAttributeValue;
import static com.amazonaws.services.sqs.util.SQSQueueUtils.longMessageAttributeValue;
import static java.util.concurrent.Executors.callable;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageSystemAttributeName;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSScheduledExecutorService extends SQSExecutorService implements ScheduledExecutorService {

    // This needs to be lower than the deduplication window to ensure that cycling the messages
    // refreshes the deduplication timeout without any race conditions.
    private static final long MAX_SQS_DELAY_SECONDS = TimeUnit.SECONDS.convert(15, TimeUnit.MINUTES);  

    public SQSScheduledExecutorService(AmazonSQSRequester sqsRequester, AmazonSQSResponder sqsResponder, String queueUrl, Consumer<Exception> exceptionHandler) {
        super(sqsRequester, sqsResponder, queueUrl, exceptionHandler);
    }

    /**
     * A scheduled version of an SQS task. Strongly modeled after
     * {@link ScheduledThreadPoolExecutor#ScheduledFutureTask}.
     */
    private class ScheduledSQSFutureTask<T> extends SQSFutureTask<T> implements ScheduledFuture<T> {

        private static final String DELAY_NANOS_ATTRIBUTE_NAME = "DelayNanos";
        private static final String PERIOD_NANOS_ATTRIBUTE_NAME = "PeriodNanos";

        private long delay;

        /**
         * Period in seconds for repeating tasks.  A positive
         * value indicates fixed-rate execution.  A negative value
         * indicates fixed-delay execution.  A value of 0 indicates a
         * non-repeating task.
         */
        private final long period;

        /** 
         * The time the task is enabled to execute in nanoTime units.
         * Only tracked for the benefit of getDelay() in the Delayed interface:
         * this implementation depends on the accuracy of the remaining delay instead. 
         */
        private long time;

        public ScheduledSQSFutureTask(Callable<T> callable, MessageContent messageContent, boolean withResponse, long delay, long period, TimeUnit unit) {
            super(callable, messageContent, withResponse);

            if (unit.ordinal() < TimeUnit.SECONDS.ordinal()) {
                throw new IllegalArgumentException("Delays at this precision not supported: " + unit);
            }
            this.delay = unit.toNanos(delay);
            this.period = unit.toNanos(period);

            messageContent.setMessageAttributesEntry(DELAY_NANOS_ATTRIBUTE_NAME, 
                    longMessageAttributeValue(this.delay));
            messageContent.setMessageAttributesEntry(PERIOD_NANOS_ATTRIBUTE_NAME, 
                    longMessageAttributeValue(this.period));

            this.time = getTime(this.delay);
        }

        public ScheduledSQSFutureTask(Message message) {
            super(message);

            this.delay = getLongMessageAttributeValue(messageContent.getMessageAttributes(), DELAY_NANOS_ATTRIBUTE_NAME).orElse(0L);
            this.period = getLongMessageAttributeValue(messageContent.getMessageAttributes(), PERIOD_NANOS_ATTRIBUTE_NAME).orElse(0L);

            decrementDelay(message);
            this.time = getTime(delay);
        }

        protected void decrementDelay(Message message) {
            long sendTimestamp = Long.parseLong(message.getAttributes().get(MessageSystemAttributeName.SentTimestamp.toString()));
            long receiveTimestamp = Long.parseLong(message.getAttributes().get(MessageSystemAttributeName.ApproximateFirstReceiveTimestamp.toString()));
            long dwellTime = receiveTimestamp - sendTimestamp;
            this.delay -= TimeUnit.NANOSECONDS.convert(dwellTime, TimeUnit.MILLISECONDS);
            messageContent.setMessageAttributesEntry(DELAY_NANOS_ATTRIBUTE_NAME, 
                    longMessageAttributeValue(delay));
        }

        private long getTime(long delay) {
            return System.nanoTime() + delay;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(time - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            if (other == this) { // compare zero if same object
                return 0;
            }
            if (other instanceof ScheduledSQSFutureTask) {
                ScheduledSQSFutureTask<?> x = (ScheduledSQSFutureTask<?>)other;
                long diff = time - x.time;
                if (diff < 0) {
                    return -1;
                } else if (diff > 0) {
                    return 1;
                } else {
                    return 0;
                }
            }
            long d = getDelay(TimeUnit.NANOSECONDS) -
                    other.getDelay(TimeUnit.NANOSECONDS);
            return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
        }

        @Override
        public SendMessageRequest toSendMessageRequest() {
            SendMessageRequest request = super.toSendMessageRequest();

            int sqsDelaySeconds = Math.max(1, (int)Math.min(TimeUnit.NANOSECONDS.toSeconds(delay), MAX_SQS_DELAY_SECONDS));
            request.setDelaySeconds(sqsDelaySeconds);

            return request;
        }

        public boolean isPeriodic() {
            return period != 0;
        }

        @Override
        public void run() {
            // Send back to the queue if the total delay hasn't elapsed yet.
            if (delay > 0) {
                send();
                return;
            } 

            this.time = System.nanoTime();

            if (!isPeriodic()) {
                ScheduledSQSFutureTask.super.run();
            } else if (ScheduledSQSFutureTask.super.runAndReset()) {
                setNextRunTime();
                send();
            }
        }

        /**
         * Sets the next time to run for a periodic task.
         */
        private void setNextRunTime() {
            long p = period;
            if (p > 0) {
                // Delay from the start of run()
                time += p;
                delay = this.time - System.nanoTime();
            } else {
                // Delay from now
                delay = -p;
                time = getTime(delay);
            }
        }
    }

    @Override
    protected SQSFutureTask<?> deserializeTask(Message message) {
        return new ScheduledSQSFutureTask<>(message);
    }

    public void delayedExecute(Runnable runnable, long delay, TimeUnit unit) {
        ScheduledSQSFutureTask<?> task = new ScheduledSQSFutureTask<>(
                callable(runnable, null), toMessageContent(runnable), false, delay, 0, unit);
        execute(task);
    }

    public void repeatWithFixedDelay(Runnable runnable, long initialDelay, long delay, TimeUnit unit) {
        ScheduledSQSFutureTask<?> task = new ScheduledSQSFutureTask<>(
                callable(runnable, null), toMessageContent(runnable), false, initialDelay, delay, unit);
        execute(task);
    }

    public void repeatAtFixedRate(Runnable runnable, long initialDelay, long delay, TimeUnit unit) {
        ScheduledSQSFutureTask<?> task = new ScheduledSQSFutureTask<>(
                callable(runnable, null), toMessageContent(runnable), false, initialDelay, -delay, unit);
        execute(task);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return schedule(callable(command, null), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        ScheduledSQSFutureTask<V> task = new ScheduledSQSFutureTask<>(
                callable, toMessageContent(callable), true, delay, 0, unit);
        execute(task);
        return task;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        ScheduledSQSFutureTask<?> task = new ScheduledSQSFutureTask<>(
                callable(command, null), toMessageContent(command), true, initialDelay, period, unit);
        execute(task);
        return task;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        ScheduledSQSFutureTask<?> task = new ScheduledSQSFutureTask<>(
                callable(command, null), toMessageContent(command), true, initialDelay, -delay, unit);
        execute(task);
        return task;
    }
}
