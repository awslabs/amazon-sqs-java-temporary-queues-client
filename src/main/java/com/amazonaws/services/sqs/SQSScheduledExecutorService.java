package com.amazonaws.services.sqs;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.MessageSystemAttributeName;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSScheduledExecutorService extends SQSExecutorService implements ScheduledExecutorService {

    // This needs to be lower than the deduplication window to ensure that cycling the messages
    // refreshes the deduplication timeout without any race conditions.
	private static final long MAX_SQS_DELAY_SECONDS = TimeUnit.SECONDS.convert(15, TimeUnit.MINUTES);  
	
	public SQSScheduledExecutorService(AmazonSQS sqs, String queueUrl) {
		super(sqs, queueUrl);
	}

	public SQSScheduledExecutorService(AmazonSQS sqs, String queueUrl, String executorID) {
		super(sqs, queueUrl, executorID);
	}

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
		
        public ScheduledSQSFutureTask(Callable<T> callable, boolean withResponse, long delay, long period, TimeUnit unit) {
        	super(callable, withResponse);
        	
        	if (unit.ordinal() < TimeUnit.SECONDS.ordinal()) {
    			throw new IllegalArgumentException("Delays at this precision not supported: " + unit);
    		}
        	this.delay = unit.toNanos(delay);
    		this.period = unit.toNanos(period);
        	
    		this.time = getTime(delay);
        }
        
        public ScheduledSQSFutureTask(Callable<T> callable, Message message) {
        	super(callable, message);
        	
        	this.delay = getLongAttributeValue(message, DELAY_NANOS_ATTRIBUTE_NAME);
        	this.period = getLongAttributeValue(message, PERIOD_NANOS_ATTRIBUTE_NAME);
        	
        	decrementDelay(message);
        	this.time = getTime(delay);
		}

        private long getLongAttributeValue(Message message, String attributeName) {
        	String value = getStringAttributeValue(message, attributeName);
        	return value != null ? Long.parseLong(value) : 0;
        }
        
        protected void decrementDelay(Message message) {
        	long sendTimestamp = Long.parseLong(message.getAttributes().get(MessageSystemAttributeName.SentTimestamp.toString()));
			long receiveTimestamp = Long.parseLong(message.getAttributes().get(MessageSystemAttributeName.ApproximateFirstReceiveTimestamp.toString()));
			long dwellTime = receiveTimestamp - sendTimestamp;
			this.delay -= TimeUnit.NANOSECONDS.convert(dwellTime, TimeUnit.MILLISECONDS);
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
		public SendMessageRequest serialize() {
			SendMessageRequest request = super.serialize();
			
			request.addMessageAttributesEntry(DELAY_NANOS_ATTRIBUTE_NAME, 
					new MessageAttributeValue().withDataType("Number").withStringValue(Long.toString(delay)));
			request.addMessageAttributesEntry(PERIOD_NANOS_ATTRIBUTE_NAME, 
					new MessageAttributeValue().withDataType("Number").withStringValue(Long.toString(period)));

			int sqsDelaySeconds = (int)Math.min(TimeUnit.NANOSECONDS.toSeconds(delay), MAX_SQS_DELAY_SECONDS);
			if (sqsDelaySeconds < 0) {
				sqsDelaySeconds = 1;
			}
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
		currentDeserializer.set(this);
		try {
			Callable<?> callable = (Callable<?>)serializer.unapply(message.getBody());
			return new ScheduledSQSFutureTask<>(callable, message);
		} finally {
			currentDeserializer.set(null);
		}
	}
	
	public void delayedExecute(Runnable runnable, long delay, TimeUnit unit) {
		ScheduledSQSFutureTask<?> task = new ScheduledSQSFutureTask<>(callable(runnable, null), false, delay, 0, unit);
		execute(task);
	}

	public void repeatWithFixedDelay(Runnable runnable, long initialDelay, long delay, TimeUnit unit) {
		ScheduledSQSFutureTask<?> task = new ScheduledSQSFutureTask<>(callable(runnable, null), false, initialDelay, delay, unit);
		execute(task);
	}

	public void repeatAtFixedRate(Runnable runnable, long initialDelay, long delay, TimeUnit unit) {
		ScheduledSQSFutureTask<?> task = new ScheduledSQSFutureTask<>(callable(runnable, null), false, initialDelay, -delay, unit);
		execute(task);
	}
	
	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		return schedule(callable(command, null), delay, unit);
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		ScheduledSQSFutureTask<V> task = new ScheduledSQSFutureTask<>(callable, true, delay, 0, unit);
		execute(task);
		return task;
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		ScheduledSQSFutureTask<?> task = new ScheduledSQSFutureTask<>(callable(command, null), true, initialDelay, period, unit);
		execute(task);
		return task;
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		ScheduledSQSFutureTask<?> task = new ScheduledSQSFutureTask<>(callable(command, null), true, initialDelay, -delay, unit);
		execute(task);
		return task;
	}
}
