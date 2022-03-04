package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.Constants;
import com.amazonaws.services.sqs.util.DaemonThreadFactory;
import com.amazonaws.services.sqs.util.ReceiveQueueBuffer;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSMessageConsumerBuilder;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsRequest;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.TagQueueRequest;
import software.amazon.awssdk.services.sqs.model.TagQueueResponse;
import software.amazon.awssdk.services.sqs.model.UntagQueueRequest;
import software.amazon.awssdk.services.sqs.model.UntagQueueResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static com.amazonaws.services.sqs.util.Constants.IDLE_QUEUE_RETENTION_PERIOD;

/**
 * An AmazonSQS wrapper that adds support for "virtual" queues, which are logical
 * queues hosted by an actual "physical" SQS queue.
 * <p>
 * Virtual queues are created by invoking {@link #createQueue(CreateQueueRequest)} with the
 * "HostQueueUrl" queue attribute set to the URL of an existing physical queue. This will create
 * in-memory buffers but will not cause any outgoing calls to SQS. The returned queue
 * URL will include both the host URL and the requested queue name, separated by a '#'. For example:
 * 
 * <pre>
 * https://sqs.us-west-2.amazonaws.com/1234566789012/MyHostQueue#MyVirtualQueue
 * </pre>
 * 
 * When a message is sent to a virtual queue URL, this client will send the message to the host queue,
 * but with an additional message attribute set to the name of the virtual queue. 
 * <p>
 * On the consuming side,
 * receives to virtual queues are multiplexed into long-poll receives on the host queue. As messages are
 * received from the host queue, a background thread will dispatch them to waiting virtual queue receives
 * according to the message attribute.
 * <p>
 * Virtual queues are also automatically deleted after a configurable period with no API calls, to avoid exhausting
 * memory if client code is failing to delete them explicitly. The "IdleQueueRetentionPeriodSeconds" queue
 * attribute configures the length of this period. See the {@link AmazonSQSIdleQueueDeletingClient} class,
 * which implements this concept for physical SQS queues, for more details.
 */
class AmazonSQSVirtualQueuesClient extends AbstractAmazonSQSClientWrapper {

    private static final Log LOG = LogFactory.getLog(AmazonSQSVirtualQueuesClient.class);

    // This is just protection against bad logic that creates unbounded queues.
    public static final int MAXIMUM_VIRTUAL_QUEUES_COUNT = 1_000_000;

    private final int hostQueuePollingThreads;

    private final int maxWaitTimeSeconds;

    // This is currently only relevant for receives, since all operations on virtual queues
    // are handled locally and heartbeats are cheap. Only receives have any wait time and therefore
    // need to heartbeat *during* the operation.
    // An alternative approach would be to remove the expiration scheduled task during the receive
    // and restore it afterwards, but I prefer the hearbeating approach because it guards against
    // threads dying or deadlocking.
    private final long heartbeatIntervalSeconds;

    private static final String VIRTUAL_QUEUE_NAME_ATTRIBUTE = "__AmazonSQSVirtualQueuesClient.QueueName";

    static final BiConsumer<String, Message> DEFAULT_ORPHANED_MESSAGE_HANDLER = (queueName, message) -> {
        LOG.warn("Orphaned message sent to " + queueName + ": " + message.messageId());
    };

    private final ConcurrentMap<String, HostQueue> hostQueues = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, VirtualQueue> virtualQueues = new ConcurrentHashMap<>();

    private final Optional<BiConsumer<String, Message>> messageHandlerOptional;
    private final BiConsumer<String, Message> orphanedMessageHandler;

    // Used to delete idle virtual queues.
    private final ScheduledExecutorService executor = createIdleQueueDeletionExecutor();

    private static ScheduledExecutorService createIdleQueueDeletionExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
                new DaemonThreadFactory("AmazonSQSVirtualQueuesClient"));
        // Since we are cancelling and resubmitting a task on every heartbeat,
        // without this setting the size of the work queue will depend on the number of
        // heartbeat calls made within the retention period, and hence on the TPS made to
        // the queue. That can lead to increased memory pressure, possibly even exhausting
        // memory.
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    AmazonSQSVirtualQueuesClient(SqsClient amazonSqsToBeExtended,
                                 Optional<BiConsumer<String, Message>> messageHandlerOptional,
                                 BiConsumer<String, Message> orphanedMessageHandler,
                                 int hostQueuePollingThreads, int maxWaitTimeSeconds,
                                 long heartbeatIntervalSeconds) {
        super(amazonSqsToBeExtended);
        this.messageHandlerOptional = Objects.requireNonNull(messageHandlerOptional);
        this.orphanedMessageHandler = Objects.requireNonNull(orphanedMessageHandler);
        this.hostQueuePollingThreads = hostQueuePollingThreads;
        this.maxWaitTimeSeconds = maxWaitTimeSeconds;
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    }

    private Optional<VirtualQueue> getVirtualQueue(String queueUrl) {
        Optional<VirtualQueueID> optionalVirtualQueueId = VirtualQueueID.fromQueueUrl(queueUrl);
        if (optionalVirtualQueueId.isPresent()) {
            VirtualQueueID virtualQueueId = optionalVirtualQueueId.get();
            Optional<VirtualQueue> result = Optional.ofNullable(virtualQueues.get(virtualQueueId.getVirtualQueueName()));
            if (!result.isPresent()) {
                throw QueueDoesNotExistException.builder().message("The specified queue does not exist").build();
            }
            return result;
        } else {
            return Optional.empty();
        }
    }
    
    @Override
    public CreateQueueResponse createQueue(CreateQueueRequest request) {
        String hostQueueUrl = request.attributesAsStrings().get(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);
        if (hostQueueUrl == null) {
            return amazonSqsToBeExtended.createQueue(request);
        }

        Map<String, String> attributes = new HashMap<>(request.attributesAsStrings());
        attributes.remove(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);

        Optional<Long> retentionPeriod = AmazonSQSIdleQueueDeletingClient.getRetentionPeriod(attributes);

        if (!attributes.isEmpty()) {
            throw new IllegalArgumentException("Virtual queues do not support setting these queue attributes independently of their host queues: "
                    + attributes.keySet());
        }

        HostQueue host = hostQueues.computeIfAbsent(hostQueueUrl, HostQueue::new);
        VirtualQueue virtualQueue = new VirtualQueue(host, request.queueName(), retentionPeriod);

        // There is clearly a race condition here between checking the size and
        // adding to the map, but that's fine since this is just a loose upper bound
        // and it avoids synchronizing all calls on something like an AtomicInteger.
        // The worse case scenario is that the map has X entries more than the maximum
        // where X is the number of threads concurrently creating queues.
        if (virtualQueues.size() > MAXIMUM_VIRTUAL_QUEUES_COUNT) {
            throw new IllegalStateException("Cannot create virtual queue: the number of virtual queues would exceed the maximum of "
                    + MAXIMUM_VIRTUAL_QUEUES_COUNT);
        }
        virtualQueues.put(virtualQueue.getID().getVirtualQueueName(), virtualQueue);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Total Virtual Queue Created is %s and Queue Name is %s", virtualQueues.size(), virtualQueue.getID().getVirtualQueueName()));
        }

        return CreateQueueResponse.builder().queueUrl(virtualQueue.getID().getQueueUrl()).build();
    }
    
    @Override
    public SendMessageResponse sendMessage(SendMessageRequest request) {
        return VirtualQueueID.fromQueueUrl(request.queueUrl())
                .map(id -> id.sendMessage(amazonSqsToBeExtended, request))
                .orElseGet(() -> amazonSqsToBeExtended.sendMessage(request));
    }

    @Override
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest request) {
        return getVirtualQueue(request.queueUrl())
                .map(virtualQueue -> virtualQueue.receiveMessage(request))
                .orElseGet(() -> amazonSqsToBeExtended.receiveMessage(request));
    }

    @Override
    public DeleteMessageResponse deleteMessage(DeleteMessageRequest request) {
        return getVirtualQueue(request.queueUrl())
                .map(virtualQueue -> virtualQueue.deleteMessage(request))
                .orElseGet(() -> amazonSqsToBeExtended.deleteMessage(request));
    }

    @Override
    public DeleteQueueResponse deleteQueue(DeleteQueueRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Deleting Virtual Queue is %s and Queue Name is %s", (virtualQueues.size() - 1), request.queueUrl()));
        }

        return getVirtualQueue(request.queueUrl())
                .map(virtualQueue -> virtualQueue.deleteQueue())
                .orElseGet(() -> amazonSqsToBeExtended.deleteQueue(request));
    }

    @Override
    public GetQueueAttributesResponse getQueueAttributes(GetQueueAttributesRequest request) {
        return getVirtualQueue(request.queueUrl())
                .map(virtualQueue -> virtualQueue.getQueueAttributes(request))
                .orElseGet(() -> amazonSqsToBeExtended.getQueueAttributes(request));
    }
    
    @Override
    public SetQueueAttributesResponse setQueueAttributes(SetQueueAttributesRequest request) {
        if (VirtualQueueID.fromQueueUrl(request.queueUrl()).isPresent()) {
            throw new IllegalArgumentException("Cannot change queue attributes of virtual queues after creation: " + request.queueUrl());
        } else {
            return amazonSqsToBeExtended.setQueueAttributes(request);
        }
    }

    @Override
    public TagQueueResponse tagQueue(TagQueueRequest request) {
        return getVirtualQueue(request.queueUrl())
                .map(virtualQueue -> virtualQueue.tagQueue(request))
                .orElseGet(() -> amazonSqsToBeExtended.tagQueue(request));
    }
    
    @Override
    public UntagQueueResponse untagQueue(UntagQueueRequest request) {
        return getVirtualQueue(request.queueUrl())
                .map(virtualQueue -> virtualQueue.untagQueue(request))
                .orElseGet(() -> amazonSqsToBeExtended.untagQueue(request));
    }
    
    @Override
    public ListQueueTagsResponse listQueueTags(ListQueueTagsRequest request) {
        return getVirtualQueue(request.queueUrl())
                .map(virtualQueue -> virtualQueue.listQueueTags())
                .orElseGet(() -> amazonSqsToBeExtended.listQueueTags(request));
    }
    
    @Override
    public void close() {
        hostQueues.values().parallelStream().forEach(HostQueue::shutdown);
        super.close();
    }

    private class HostQueue {

        private final String queueUrl;
        private final ReceiveQueueBuffer buffer;
        private final SQSMessageConsumer consumer;

        public HostQueue(String queueUrl) {
            this.queueUrl = queueUrl;
            // Used to avoid repeatedly fetching the default visibility timeout and receive message
            // wait time on the queue.
            this.buffer = new ReceiveQueueBuffer(amazonSqsToBeExtended, executor, queueUrl);

            this.consumer = SQSMessageConsumerBuilder.standard()
                                                     .withAmazonSQS(AmazonSQSVirtualQueuesClient.this)
                                                     .withQueueUrl(queueUrl)
                                                     .withConsumer(this::dispatchMessage)
                                                     .withMaxWaitTimeSeconds(maxWaitTimeSeconds)
                                                     .withPollingThreadCount(hostQueuePollingThreads)
                                                     .build();
            this.consumer.start();
        }

        private void dispatchMessage(Message message) {
            MessageAttributeValue messageAttributeValue = message.messageAttributes().get(VIRTUAL_QUEUE_NAME_ATTRIBUTE);
            // Case where a message was sent with missing attribute __AmazonSQSVirtualQueuesClient.QueueName
            if (messageAttributeValue == null) {
                orphanedMessageHandler.accept(queueUrl, message);
                return;
            }
            String queueName = messageAttributeValue.stringValue();
            VirtualQueue virtualQueue = virtualQueues.get(queueName);
            if (virtualQueue != null) {
                messageHandlerOptional.map(messageHandler -> {
                    messageHandler.accept(virtualQueue.getID().getQueueUrl(), message);
                    virtualQueue.heartbeat();
                    return null;
                }).orElseGet(() -> {
                    virtualQueue.receiveBuffer.deliverMessages(Collections.singletonList(message), queueUrl, null);
                    return null;
                });
            } else {
                orphanedMessageHandler.accept(queueName, message);
            }
        }

        public void shutdown() {
            this.buffer.shutdown();
            this.consumer.terminate();
        }
    }

    private class VirtualQueue {
        
        private final VirtualQueueID id;
        private final HostQueue hostQueue;
        private final ConcurrentMap<String, String> tags = new ConcurrentHashMap<>();
        private final ReceiveQueueBuffer receiveBuffer;
        private final Optional<Long> retentionPeriod;
        private Optional<ScheduledFuture<?>> expireFuture;

        public VirtualQueue(HostQueue hostQueue, String queueName, Optional<Long> retentionPeriod) {
            this.id = new VirtualQueueID(hostQueue.queueUrl, queueName);
            this.hostQueue = hostQueue;
            this.receiveBuffer = new ReceiveQueueBuffer(hostQueue.buffer);
            this.retentionPeriod = retentionPeriod;
            this.expireFuture = Optional.empty();
            heartbeat();
        }
        
        public VirtualQueueID getID() {
            return id;
        }
        
        public GetQueueAttributesResponse getQueueAttributes(GetQueueAttributesRequest request) {
            List<String> attributeNames = new ArrayList<>(request.attributeNamesAsStrings());
            boolean includeHostQueue = 
                    attributeNames.remove(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE) ||
                    attributeNames.contains("All");
            boolean includeRetentionPeriod = retentionPeriod.isPresent() && 
                    (attributeNames.contains(IDLE_QUEUE_RETENTION_PERIOD) ||
                     attributeNames.contains("All"));
            
            GetQueueAttributesRequest hostQueueRequest = GetQueueAttributesRequest.builder()
                    .queueUrl(hostQueue.queueUrl)
                    .attributeNamesWithStrings(attributeNames).build();
            GetQueueAttributesResponse result = amazonSqsToBeExtended.getQueueAttributes(hostQueueRequest);
            Map<String, String> attributesAsStrings = new HashMap<>(result.attributesAsStrings());
            if (includeHostQueue) {
                attributesAsStrings.put(Constants.VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueue.queueUrl);
                result = result.toBuilder().attributesWithStrings(attributesAsStrings).build();
            }
            if (includeRetentionPeriod) {
                attributesAsStrings.put(IDLE_QUEUE_RETENTION_PERIOD, retentionPeriod.get().toString());
                result = result.toBuilder().attributesWithStrings(attributesAsStrings).build();
            }
            return result;
        }
        
        public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest request) {
            heartbeat();
            try {
                try {
                    Future<ReceiveMessageResponse> future = receiveBuffer.receiveMessageAsync(request);
                    long waitTimeSeconds = Optional.ofNullable(request.waitTimeSeconds()).orElse(0).longValue();
                    // Necessary to ensure the loop terminates
                    if (waitTimeSeconds < 0) {
                        throw new IllegalArgumentException("WaitTimeSeconds cannot be negative: " + waitTimeSeconds);
                    }
                    do {
                        long waitTimeBeforeHeartBeat = Math.min(heartbeatIntervalSeconds, waitTimeSeconds);
                        try {
                            return future.get(waitTimeBeforeHeartBeat, TimeUnit.SECONDS);
                        } catch (TimeoutException e) {
                            // Fall through
                        }
                        heartbeat();
                        waitTimeSeconds -= waitTimeBeforeHeartBeat;
                    } while (waitTimeSeconds > 0);
                } catch (ExecutionException e) {
                    throw (RuntimeException)e.getCause();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
    
                // Empty receive
                return ReceiveMessageResponse.builder().build();
            } finally {
                heartbeat();
            }
        }
        
        public DeleteMessageResponse deleteMessage(DeleteMessageRequest request) {
            heartbeat();
            DeleteMessageRequest virtualQueueResult = DeleteMessageRequest.builder()
                    .queueUrl(id.getHostQueueUrl())
                    .receiptHandle(request.receiptHandle()).build();
            return amazonSqsToBeExtended.deleteMessage(virtualQueueResult);
        }
        
        public void heartbeat() {
            expireFuture.ifPresent(f -> f.cancel(false));
            expireFuture = retentionPeriod.map(period ->
                    executor.schedule(this::deleteIdleQueue, period, TimeUnit.SECONDS));
        }
        
        public DeleteQueueResponse deleteQueue() {
            virtualQueues.remove(id.getVirtualQueueName());
            receiveBuffer.shutdown();
            expireFuture.ifPresent(f -> f.cancel(false));
            return DeleteQueueResponse.builder().build();
        }

        private void deleteIdleQueue() {
            LOG.info("Deleting idle virtual queue: " + id.getQueueUrl());
            deleteQueue();
        }

        public TagQueueResponse tagQueue(TagQueueRequest request) {
            tags.putAll(request.tags());
            return TagQueueResponse.builder().build();
        }
        
        public UntagQueueResponse untagQueue(UntagQueueRequest request) {
            tags.keySet().removeAll(request.tagKeys());
            return UntagQueueResponse.builder().build();
        }
        
        public ListQueueTagsResponse listQueueTags() {
            return ListQueueTagsResponse.builder().tags(tags).build();
        }
    }
    
    private static class VirtualQueueID {

        private final String hostQueueUrl;
        private final String virtualQueueName;

        public VirtualQueueID(String hostQueueUrl, String virtualQueueName) {
            this.hostQueueUrl = hostQueueUrl;
            this.virtualQueueName = virtualQueueName;
        }

        public static Optional<VirtualQueueID> fromQueueUrl(String queueUrl) {
            int index = queueUrl.indexOf('#');
            if (index >= 0) {
                return Optional.of(new VirtualQueueID(queueUrl.substring(0, index), queueUrl.substring(index + 1)));
            } else {
                return Optional.empty();
            }
        }

        public String getHostQueueUrl() {
            return hostQueueUrl;
        }

        public String getVirtualQueueName() {
            return virtualQueueName;
        }

        public String getQueueUrl() {
            return hostQueueUrl + '#' + virtualQueueName;
        }
        
        public SendMessageResponse sendMessage(SqsClient sqs, SendMessageRequest request) {
            SendMessageRequest virtualQueueRequest = SQSQueueUtils.copyWithExtraAttributes(request, Collections.singletonMap(VIRTUAL_QUEUE_NAME_ATTRIBUTE, 
                    MessageAttributeValue.builder().dataType("String").stringValue(virtualQueueName).build()));
            virtualQueueRequest = virtualQueueRequest.toBuilder().queueUrl(hostQueueUrl).build();
            return sqs.sendMessage(virtualQueueRequest);
        }
    }
}
