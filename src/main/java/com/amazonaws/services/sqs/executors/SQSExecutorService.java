package com.amazonaws.services.sqs.executors;

import static com.amazonaws.services.sqs.util.SQSQueueUtils.booleanMessageAttributeValue;
import static com.amazonaws.services.sqs.util.SQSQueueUtils.getBooleanMessageAttributeValue;
import static com.amazonaws.services.sqs.util.SQSQueueUtils.getStringMessageAttributeValue;
import static com.amazonaws.services.sqs.util.SQSQueueUtils.stringMessageAttributeValue;
import static com.amazonaws.util.StringUtils.UTF8;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.responsesapi.AmazonSQSWithResponses;
import com.amazonaws.services.sqs.responsesapi.MessageContent;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
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
	
	public SQSExecutorService(AmazonSQSWithResponses sqs, String queueUrl) {
		this(sqs, queueUrl, queueUrl, DefaultSerializer.INSTANCE.andThen(Base64Serializer.INSTANCE));
	}

	public SQSExecutorService(AmazonSQSWithResponses sqs, String queueUrl, String executorID) {
	    this(sqs, queueUrl, executorID, DefaultSerializer.INSTANCE.andThen(Base64Serializer.INSTANCE));
	}
 
	public SQSExecutorService(AmazonSQSWithResponses sqs, String queueUrl, String executorID, InvertibleFunction<Object, String> serializer) {
		this.sqs = sqs.getAmazonSQS();
	    this.sqsResponseClient = sqs;
		this.queueUrl = queueUrl;
		this.messageConsumer = new SQSMessageConsumer(this.sqs, queueUrl, this::accept);
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
		private final Optional<String> deduplicationID;
	    private String uuid;
		private final long expiry;
		private Optional<String> serializedResult;
		
		public Metadata(Optional<String> deduplicationID, String uuid) {
			// TODO-RS: Clock drift!
			this(deduplicationID, uuid, System.currentTimeMillis() + DEDUPLICATION_WINDOW_MILLIS, Optional.empty());
		}
		
		private Metadata(Optional<String> deduplicationID, String uuid, long expiry, Optional<String> serializedResult) {
			this.deduplicationID = deduplicationID;
		    this.uuid = uuid;
			this.expiry = expiry;
			this.serializedResult = serializedResult;
		}
		
		public static Metadata fromTag(String serialized) {
		    String[] parts = serialized.split(":");
			return new Metadata(Optional.of(parts[0]).filter(s -> !"null".equals(s)), 
			                    parts[1], 
			                    Long.parseLong(parts[2]),
			                    Optional.of(parts[3]).filter(s -> !"null".equals(s)));
		}
		
		public static Metadata fromMessageContent(MessageContent messageContent) {
		    String uuid = getStringMessageAttributeValue(messageContent.getMessageAttributes(), SQSFutureTask.UUID_ATTRIBUTE_NAME).get();
		    Optional<String> deduplicationID = getStringMessageAttributeValue(messageContent.getMessageAttributes(), SQSFutureTask.DEDUPLICATION_ID_ATTRIBUTE_NAME);
            return new Metadata(deduplicationID, uuid);
        }
        
        public boolean shouldNotRun(SQSFutureTask<?> task) {
			// TODO-RS: Leverage SentTimestamp and ApproximateFirstReceiveTimestamp
			// to fight clock drift.
            if (isExpired()) {
                return false;
            } else if (isDuplicate(task)) {
                return true;
            } else if (serializedResult.isPresent()) {
                return true;
            } else {
                return false;
            }
		}
		
        public boolean isDuplicate(SQSFutureTask<?> task) {
            return deduplicationID.isPresent() && !uuid.equals(task.metadata.uuid);
        }
        
		public boolean isExpired() {
			return System.currentTimeMillis() > expiry;
		}
		
		public void saveToTag(AmazonSQS sqs, String queueUrl) {
		    String key = deduplicationID.orElse(uuid);
		    sqs.tagQueue(queueUrl, Collections.singletonMap(key, toString()));
		}
		
		@Override
		public String toString() {
		    // TODO-RS: clean up serialization
			StringBuilder builder = new StringBuilder();
			builder.append(deduplicationID.orElse("null"));
            builder.append(':');
            builder.append(uuid);
			builder.append(':');
			builder.append(expiry);
		    builder.append(':');
            builder.append(serializedResult.orElse("null"));
			return builder.toString();
		}
	}
	
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
	    MessageContent messageContent = toMessageContent(callable);
	    addDeduplicationAttributes(messageContent, callable);
        return new SQSFutureTask<>(callable, messageContent, true);
	}
	
	@Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return newTaskFor(runnable, value, true);
    }
    
	private <T> SQSFutureTask<T> newTaskFor(Runnable runnable, T value, boolean withResponse) {
	    MessageContent messageContent = toMessageContent(runnable);
	    addDeduplicationAttributes(messageContent, runnable);
        return new SQSFutureTask<>(Executors.callable(runnable, value), messageContent, withResponse);
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
			return new SQSFutureTask<>(message);
		} finally {
			currentDeserializer.set(null);
		}
	}
	
	public SQSFutureTask<?> convert(Runnable runnable) {
		if (runnable instanceof SQSFutureTask<?>) {
			return (SQSFutureTask<?>)runnable;
		} else {
			return newTaskFor(runnable, null, false);
		}
	}
	
	protected MessageContent toMessageContent(Runnable runnable) {
	    MessageContent messageContent = new MessageContent(serializer.apply(runnable));
	    addDeduplicationAttributes(messageContent, runnable);
	    return messageContent;
	}
	
	protected MessageContent toMessageContent(Callable<?> callable) {
	    MessageContent messageContent = new MessageContent(serializer.apply(callable));
	    messageContent.setMessageAttributesEntry(SQSFutureTask.IS_CALLABLE_ATTRIBUTE_NAME, booleanMessageAttributeValue(true));
	    addDeduplicationAttributes(messageContent, callable);
        return messageContent;
    }
    
	private void addDeduplicationAttributes(MessageContent messageContent, Object task) {
	    if (task instanceof Deduplicated) {
	        String deduplicationID = ((Deduplicated)task).deduplicationID();
            if (deduplicationID == null) {
                String body = messageContent.getMessageBody();
                deduplicationID = BinaryUtils.toHex(Md5Utils.computeMD5Hash(body.getBytes(UTF8)));
            }
            messageContent.setMessageAttributesEntry(SQSFutureTask.DEDUPLICATION_ID_ATTRIBUTE_NAME,
                    stringMessageAttributeValue(deduplicationID));
	    }
	    messageContent.setMessageAttributesEntry(SQSFutureTask.UUID_ATTRIBUTE_NAME,
                stringMessageAttributeValue(UUID.randomUUID().toString()));
	}
	
    @SuppressWarnings("unchecked")
    private <T> Callable<T> callableFromMessage(Message message) {
        Object deserialized = serializer.unapply(message.getBody());
        boolean isCallable = getBooleanMessageAttributeValue(message.getMessageAttributes(), SQSFutureTask.IS_CALLABLE_ATTRIBUTE_NAME);
        if (isCallable) {
            return (Callable<T>)deserialized;
        } else {
            return Executors.callable((Runnable)deserialized, null);
        }
    }
    
	// TODO-RS: Make this static?
	protected class SQSFutureTask<T> extends FutureTask<T> {

		private static final String DEDUPLICATION_ID_ATTRIBUTE_NAME = "DeduplicationID";
		private static final String UUID_ATTRIBUTE_NAME = "UUID";
		private static final String IS_CALLABLE_ATTRIBUTE_NAME = "IsCallable";
        
		private final Metadata metadata;
		
		private final boolean withResponse;
		protected final InvertibleFunction<Future<T>, String> futureSerializer = new CompletedFutureToMessageSerializer<>(serializer);
	    
		protected final MessageContent messageContent;
		
		// TODO-RS: The result will come either from a response message or
		// polling the deduplication metadata on the tags.
		// Is there a good way to have the same thread pool do one or the other?
		private Optional<Future<?>> resultFuture;
		
		public SQSFutureTask(Callable<T> callable, MessageContent messageContent, boolean withResponse) {
			super(callable);
			this.messageContent = messageContent;
			this.withResponse = withResponse;
			this.metadata = Metadata.fromMessageContent(messageContent);
			this.resultFuture = Optional.empty();
		}

		public SQSFutureTask(Message message) {
			super(callableFromMessage(message));
			this.messageContent = MessageContent.fromMessage(message);
			this.withResponse = sqsResponseClient.isResponseMessageRequested(messageContent);
			this.metadata = Metadata.fromMessageContent(messageContent);
			this.resultFuture = Optional.empty();
		}
		
		private Optional<Metadata> getMetadataFromTags() {
			Map<String, String> tags = sqs.listQueueTags(queueUrl).getTags();
			return Optional.ofNullable(tags.get(metadata.deduplicationID.orElse(metadata.uuid)))
			               .map(Metadata::fromTag);
		}
		
		protected void send() {
		    if (getMetadataFromTags().filter(existingMetadata -> existingMetadata.shouldNotRun(this)).isPresent()) {
		        if (withResponse) {
    		        // This will immediately complete the future and cancel itself if the metadata
    		        // already has the result set.
		            resultFuture = Optional.of(dedupedResultPoller.scheduleWithFixedDelay(
    			            this::pollForResultFromMetadata, 0, 2, TimeUnit.SECONDS));
		        }
				return;
		    }
			
			SendMessageRequest request = toSendMessageRequest();
			
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
			    this.resultFuture = Optional.of(responseFuture);
			} else {
				sqs.sendMessage(request);
			}
			
			// Tag afterwards, so that the race condition will result in duplicate receives rather than
			// potentially deduping all copies.
			if (metadata.deduplicationID.isPresent()) {
			    metadata.saveToTag(sqs, queueUrl);
			}
		}
		
		public SendMessageRequest toSendMessageRequest() {
			return messageContent.toSendMessageRequest().withQueueUrl(queueUrl);
		}
		
		private void pollForResultFromMetadata() {
		    Optional<Metadata> tagMetadata = getMetadataFromTags();
		    if (tagMetadata.isPresent()) {
		        tagMetadata.get().serializedResult.ifPresent(this::setFromResponse);
		    } else { 
                setException(new TimeoutException());
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
		    Optional<Metadata> maybeMetadata = getMetadataFromTags();
            if (maybeMetadata.filter(existingMetadata -> existingMetadata.shouldNotRun(this)).isPresent()) {
	            maybeMetadata.get().serializedResult.ifPresent(this::setFromResponse);
		        return;
		    }

		    super.run();
		}
		
		@Override
		protected void done() {
		    resultFuture.ifPresent(f -> f.cancel(false));
		    
		    String response = futureSerializer.apply(this);
            
			if (withResponse) {
	            sqsResponseClient.sendResponseMessage(messageContent, new MessageContent(response));
			}
			
			if (metadata.deduplicationID.isPresent() || isCancelled()) {
                metadata.serializedResult = Optional.of(response);
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
