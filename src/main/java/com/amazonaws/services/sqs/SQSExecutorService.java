package com.amazonaws.services.sqs;

import static com.amazonaws.util.StringUtils.UTF8;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.proxy.Base64Serializer;
import com.amazonaws.services.sqs.proxy.CompletedFutureToMessageSerializer;
import com.amazonaws.services.sqs.proxy.DefaultSerializer;
import com.amazonaws.services.sqs.proxy.InvertibleFunction;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;
import com.amazonaws.services.sqs.responsesapi.MessageContent;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.Md5Utils;

// TODO-RS: Define separate Message class instead of reusing the SQS SDK's class
// (which is really a ReceivedMessage model)?
// TODO-RS: Factor out deduplication implementation?
@SuppressWarnings("squid:S2055")
public class SQSExecutorService extends AbstractExecutorService implements Serializable {

	private static final long serialVersionUID = -3415824864452374276L;
	
	// TODO-RS: Configuration
	private static final int MAX_WAIT_TIME_SECONDS = 60;
	
	private static final long DEDUPLICATION_WINDOW_MILLIS = TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES);
	
	protected static final ThreadLocal<SQSExecutorService> currentDeserializer = new ThreadLocal<>();
	protected final transient InvertibleFunction<Object, String> serializer;
	
	private static final ConcurrentMap<String, SQSExecutorService> executorsByID = new ConcurrentHashMap<>();
	private final String executorID; 
    private final Reference reference;	
	
    protected final transient AmazonSQS sqs;
    protected final transient AmazonSQSWithResponses sqsResponseClient;
	protected final String queueUrl;
	private final transient SQSMessageConsumer messageConsumer;
	
	private final transient ScheduledExecutorService dedupedResultPoller = Executors.newScheduledThreadPool(1);
	
	private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
	
	public SQSExecutorService(AmazonSQS sqs, String queueUrl) {
		this(sqs, queueUrl, queueUrl, DefaultSerializer.INSTANCE.andThen(Base64Serializer.INSTANCE));
	}

	public SQSExecutorService(AmazonSQS sqs, String queueUrl, String executorID) {
	    this(sqs, queueUrl, executorID, DefaultSerializer.INSTANCE.andThen(Base64Serializer.INSTANCE));
	}
 
	public SQSExecutorService(AmazonSQS sqs, String queueUrl, String executorID, InvertibleFunction<Object, String> serializer) {
		this.sqs = sqs;
		this.sqsResponseClient = new AmazonSQSResponsesClient(sqs);
		this.queueUrl = queueUrl;
		this.messageConsumer = new SQSMessageConsumer(sqs, queueUrl, this::accept);
		this.executorID = executorID;
		this.reference = new Reference(executorID);

		if (executorID != null) {
			SQSExecutorService existingExecutor = executorsByID.putIfAbsent(executorID, this);
			if (existingExecutor != null) {
				throw new IllegalStateException("An SQSExecutorService has already been created with the ID " + executorID + ": " + existingExecutor);
			}
		}
		this.messageConsumer.start();
		
		this.serializer = serializer;
	}

	@Override
	public void execute(Runnable runnable) {
		if (isShutdown()) {
			throw new RejectedExecutionException("Task " + runnable.toString() +
                    " rejected from " + this.toString());
		}
		convert(runnable).send();
	}

	public void execute(SerializableRunnable runnable) {
		execute((Runnable)runnable);
	}

	// TODO-RS: Local repeating task to remove these when expired?
	private static class Metadata {
		private final String deduplicationID;
	    private String uuid;
		private final long expiry;
		private String serializedResult;
		
		public Metadata(String deduplicationID, String uuid) {
			// TODO-RS: Clock drift!
			this(deduplicationID, uuid, System.currentTimeMillis() + DEDUPLICATION_WINDOW_MILLIS, null);
		}
		
		private Metadata(String deduplicationID, String uuid, long expiry, String serializedResult) {
			this.deduplicationID = deduplicationID;
		    this.uuid = uuid;
			this.expiry = expiry;
			this.serializedResult = serializedResult;
		}
		
		public static Metadata fromTag(String serialized) {
		    String[] parts = serialized.split(":");
			return new Metadata(parts[0].equals("null") ? null : parts[0], 
			                    parts[1], 
			                    Long.parseLong(parts[2]),
			                    parts[3].equals("null") ? null : parts[3]);
		}
		
		public boolean isDuplicate(SQSFutureTask<?> task) {
			// TODO-RS: Leverage SentTimestamp and ApproximateFirstReceiveTimestamp
			// to fight clock drift.
			return !isExpired() && deduplicationID != null && !uuid.equals(task.metadata.uuid);
		}
		
		public boolean isExpired() {
			return System.currentTimeMillis() > expiry;
		}
		
		public void saveToTag(AmazonSQS sqs, String queueUrl) {
		    String key = deduplicationID != null ? deduplicationID : uuid;
		    // TODO-RS: clean up serialization
		    sqs.tagQueue(queueUrl, Collections.singletonMap(key, toString()));
		}
		
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append(deduplicationID);
            builder.append(':');
            builder.append(uuid);
			builder.append(':');
			builder.append(expiry);
		    builder.append(':');
            builder.append(serializedResult);
			return builder.toString();
		}
	}
	
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
		return new SQSFutureTask<>(callable, true);
	}
	
    /**
     * A callable that runs given task and returns given result
     */
    static final class SerializableRunnableAdapter<T> implements SerializableCallable<T> {
		private static final long serialVersionUID = 6853711467712316443L;
		final Runnable task;
        final T result;
        SerializableRunnableAdapter(Runnable task, T result) {
            this.task = task;
            this.result = result;
        }
        
        @Override
        public T call() {
            task.run();
            return result;
        }
    }
	
    protected static <T> SerializableCallable<T> callable(Runnable runnable, T value) {
    	SerializableCallable<T> result = new SerializableRunnableAdapter<>(runnable, value);
    	if (runnable instanceof DeduplicatedRunnable) {
    		return DeduplicatedCallable.deduplicated(result, ((DeduplicatedRunnable)runnable).deduplicationID());
    	} else {
    		return result;
    	}
    }
    
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
		return new SQSFutureTask<>(callable(runnable, value), true);
	}
	
	protected void accept(Message message) {
		deserializeTask(message).run();
	}
	
	protected void afterExecute(Runnable r, Throwable t) {
		// To be overridden by subclasses
	}
	
	protected SQSFutureTask<?> deserializeTask(Message message) {
		currentDeserializer.set(this);
		try {
			Callable<?> callable = (Callable<?>)serializer.unapply(message.getBody());
			return new SQSFutureTask<>(callable, message);
		} finally {
			currentDeserializer.set(null);
		}
	}
	
	public SQSFutureTask<?> convert(Runnable runnable) {
		if (runnable instanceof SQSFutureTask<?>) {
			return (SQSFutureTask<?>)runnable;
		} else {
			return new SQSFutureTask<>(callable(runnable, null), false);
		}
	}
	
	// TODO-RS: Make this static?
	protected class SQSFutureTask<T> extends FutureTask<T> {

		private static final String DEDUPLICATION_ID_ATTRIBUTE_NAME = "DeduplicationID";
		private static final String UUID_ATTRIBUTE_NAME = "UUID";
		
		protected final Callable<T> callable;
		
		private Metadata metadata;
		
		private final boolean withResponse;
		protected final InvertibleFunction<Future<T>, String> futureSerializer = new CompletedFutureToMessageSerializer<>(serializer);
	    
		private MessageContent requestMessageContent;
		
		// TODO-RS: The response will come either from a message or the deduplication metadata on the tags.
		// Is there a good way to have the same thread pool do one or the other?
		private Future<?> resultFuture;
		
		public SQSFutureTask(Callable<T> callable, boolean withResponse) {
			super(callable);
			this.callable = callable;
			this.withResponse = withResponse;
			String deduplicationID = null;
			if (callable instanceof Deduplicated) {
				deduplicationID = ((Deduplicated)callable).deduplicationID();
				if (deduplicationID == null) {
					try {
						String serializedCallable = serializer.apply(callable);
						deduplicationID = BinaryUtils.toHex(Md5Utils.computeMD5Hash(serializedCallable.getBytes(UTF8)));
					} catch (Exception e) {
						throw new RejectedExecutionException(e);
					}
				}
			}
			this.metadata = new Metadata(deduplicationID, UUID.randomUUID().toString());
		}

		public SQSFutureTask(Callable<T> callable, Message message) {
			super(callable);
			this.callable = callable;
			this.requestMessageContent = MessageContent.fromMessage(message);
			this.withResponse = sqsResponseClient.isResponseMessageRequested(requestMessageContent);
			String uuid = getStringAttributeValue(message, UUID_ATTRIBUTE_NAME);
            String deduplicationID = getStringAttributeValue(message, DEDUPLICATION_ID_ATTRIBUTE_NAME);
		    metadata = new Metadata(deduplicationID, uuid);
		}
		
		private Metadata getMetadataFromTags() {
			Map<String, String> tags = sqs.listQueueTags(queueUrl).getTags();
			String serializedValue;
			if (metadata.deduplicationID != null) {
			    serializedValue = tags.get(metadata.deduplicationID);
			} else {
			    serializedValue = tags.get(metadata.uuid);
			}
			return serializedValue != null ? Metadata.fromTag(serializedValue) : null;
		}
		
		protected void send() {
		    Metadata existingMetadata = getMetadataFromTags();
		    if (existingMetadata != null && (existingMetadata.isDuplicate(this) || existingMetadata.serializedResult != null)) {
		        if (withResponse) {
    		        // This will immediately complete the future and cancel itself if the metadata
    		        // already has the result set.
		            resultFuture = dedupedResultPoller.scheduleWithFixedDelay(
    			            this::pollForResultFromMetadata, 0, 2, TimeUnit.SECONDS);
		        }
				return;
		    }
			
			SendMessageRequest request = serialize();
			
			if (withResponse) {
			    CompletableFuture<Message> responseFuture = sqsResponseClient.sendMessageAndGetResponseAsync(
			            request, MAX_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
			    responseFuture.whenComplete((result, exception) -> {
			        if (exception != null) {
			            setException(exception);
			        } else {
			            setFromResponse(result.getBody());
			        }
			    });
			    this.resultFuture = responseFuture;
			} else {
				sqs.sendMessage(request);
			}
			
			// Tag afterwards, so that the race condition will result in duplicate receives rather than
			// potentially deduping all copies.
			if (metadata.deduplicationID != null) {
			    metadata.saveToTag(sqs, queueUrl);
			}
		}
		
		public SendMessageRequest serialize() {
			SendMessageRequest request = new SendMessageRequest()
					.withQueueUrl(queueUrl)
					.withMessageBody(serializer.apply(callable));
			if (metadata != null) {
			    request.addMessageAttributesEntry(UUID_ATTRIBUTE_NAME, stringAttributeValue(metadata.uuid));
			    if (metadata.deduplicationID != null) {
			        request.addMessageAttributesEntry(DEDUPLICATION_ID_ATTRIBUTE_NAME, stringAttributeValue(metadata.deduplicationID));
			    }
			}
			return request;
		}
		
		private MessageAttributeValue stringAttributeValue(String value) {
			return new MessageAttributeValue().withDataType("String").withStringValue(value);
		}
		
		protected String getStringAttributeValue(Message message, String attributeName) {
			MessageAttributeValue value = message.getMessageAttributes().get(attributeName);
			return value != null ? value.getStringValue() : null; 
		}
		
		private void pollForResultFromMetadata() {
		    Metadata metadata = getMetadataFromTags();
		    if (metadata == null) {
		        // TODO-RS: This should be a timeout instead
		        cancel(false);
		    } else if (metadata.serializedResult != null) {
		        setFromResponse(metadata.serializedResult);
		    }
		}
		
		private void setFromResponse(String serializedFuture) {
		    Future<T> future = futureSerializer.unapply(serializedFuture);
		    try {
                set(future.get());
            } catch (InterruptedException e) {
                // Shouldn't happen
                throw new IllegalStateException(e);
            } catch (CancellationException e) {
                cancel(false);
            } catch (ExecutionException e) {
                setException(e.getCause());
            }
		}
		
		@Override
		public void run() {
		    Metadata existingMetadata = getMetadataFromTags();
		    if (existingMetadata != null) {
		        if (existingMetadata.isDuplicate(this)) {
		            return;
		        } else if (existingMetadata.serializedResult != null) {
		            setFromResponse(existingMetadata.serializedResult);
	            }
		    }

			super.run();
		}
		
		@Override
		protected void done() {
		    if (resultFuture != null) {
		        resultFuture.cancel(false);
		    }
		    
		    String response = futureSerializer.apply(this);
            
			if (requestMessageContent != null && sqsResponseClient.isResponseMessageRequested(requestMessageContent)) {
	            MessageContent responseMessage = new MessageContent(response);
	            sqsResponseClient.sendResponseMessage(requestMessageContent, responseMessage);
			}
			
			if (metadata.deduplicationID != null || isCancelled()) {
                metadata.serializedResult = response;
                metadata.saveToTag(sqs, queueUrl);
            }
		}
	}
	
	@Override
	public void shutdown() {
		shuttingDown.set(true);
		if (executorID != null) {
		    executorsByID.remove(reference.executorID);
		}
	}
	
	@Override
	public List<Runnable> shutdownNow() {
		shutdown();
		messageConsumer.shutdown();
		return Collections.emptyList();
	}

	@Override
    public boolean isShutdown() {
		return shuttingDown.get();
    }

	@Override
    public boolean isTerminated() {
    	return isShutdown() && SQSQueueUtils.isQueueEmpty(sqs, queueUrl);
    }
    
	@Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    	return SQSQueueUtils.awaitEmptyQueue(sqs, queueUrl, timeout, unit);
	}
	
	private static class Reference implements Serializable {
		
		private static final long serialVersionUID = -374968019686668634L;
		
		private final String executorID;
		
		private Reference(String executorID) {
			this.executorID = executorID;
		}
		
		private Object readResolve() throws ObjectStreamException {
			SQSExecutorService executor;
			if (executorID != null) {
				executor = executorsByID.get(executorID); 
				if (executor == null) {
					throw new IllegalStateException("Could not locate SQSExecutor with ID " + executorID);
				}
			} else {
				executor = currentDeserializer.get();
				if (executor == null) {
					throw new IllegalStateException("Not currently in the control flow of SQSExecutor#deserializeTask");
				}
			}
			return executor;
		}
	}
	
	protected Object writeReplace() throws ObjectStreamException {
		return reference;
	}
}
