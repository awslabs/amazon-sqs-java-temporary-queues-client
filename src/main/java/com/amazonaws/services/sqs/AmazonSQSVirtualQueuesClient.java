package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ListQueueTagsRequest;
import com.amazonaws.services.sqs.model.ListQueueTagsResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import com.amazonaws.services.sqs.model.TagQueueRequest;
import com.amazonaws.services.sqs.model.TagQueueResult;
import com.amazonaws.services.sqs.model.UntagQueueRequest;
import com.amazonaws.services.sqs.model.UntagQueueResult;
import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.DaemonThreadFactory;
import com.amazonaws.services.sqs.util.ReceiveQueueBuffer;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSMessageConsumerBuilder;
import com.amazonaws.services.sqs.util.SQSQueueUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static com.amazonaws.services.sqs.AmazonSQSIdleQueueDeletingClient.IDLE_QUEUE_RETENTION_PERIOD;

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

    public static final String VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE = "HostQueueUrl";

    // This is just protection against bad logic that creates unbounded queues.
    public static final int MAXIMUM_VIRTUAL_QUEUES_COUNT = 1_000_000;

    private final int hostQueuePollingThreads;

    private final int maxWaitTimeSeconds;

    private static final String VIRTUAL_QUEUE_NAME_ATTRIBUTE = "__AmazonSQSVirtualQueuesClient.QueueName";

    static final BiConsumer<String, Message> DEFAULT_ORPHANED_MESSAGE_HANDLER = (queueName, message) -> {
        LOG.warn("Orphaned message sent to " + queueName + ": " + message.getMessageId());
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

    AmazonSQSVirtualQueuesClient(AmazonSQS amazonSqsToBeExtended,
                                 Optional<BiConsumer<String, Message>> messageHandlerOptional,
                                 BiConsumer<String, Message> orphanedMessageHandler,
                                 int hostQueuePollingThreads, int maxWaitTimeSeconds) {
        super(amazonSqsToBeExtended);
        this.messageHandlerOptional = Objects.requireNonNull(messageHandlerOptional);
        this.orphanedMessageHandler = Objects.requireNonNull(orphanedMessageHandler);
        this.hostQueuePollingThreads = hostQueuePollingThreads;
        this.maxWaitTimeSeconds = maxWaitTimeSeconds;
    }

    private Optional<VirtualQueue> getVirtualQueue(String queueUrl) {
        Optional<VirtualQueueID> optionalVirtualQueueId = VirtualQueueID.fromQueueUrl(queueUrl);
        if (optionalVirtualQueueId.isPresent()) {
            VirtualQueueID virtualQueueId = optionalVirtualQueueId.get();
            Optional<VirtualQueue> result = Optional.ofNullable(virtualQueues.get(virtualQueueId.getVirtualQueueName()));
            if (!result.isPresent()) {
                throw new QueueDoesNotExistException("The specified queue does not exist");
            }
            return result;
        } else {
            return Optional.empty();
        }
    }
    
    @Override
    public CreateQueueResult createQueue(CreateQueueRequest request) {
        String hostQueueUrl = request.getAttributes().get(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);
        if (hostQueueUrl == null) {
            return amazonSqsToBeExtended.createQueue(request);
        }

        Map<String, String> attributes = new HashMap<>(request.getAttributes());
        attributes.remove(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);

        Optional<Long> retentionPeriod = AmazonSQSIdleQueueDeletingClient.getRetentionPeriod(attributes);

        if (!attributes.isEmpty()) {
            throw new IllegalArgumentException("Virtual queues do not support setting these queue attributes independently of their host queues: "
                    + attributes.keySet());
        }

        HostQueue host = hostQueues.computeIfAbsent(hostQueueUrl, HostQueue::new);
        VirtualQueue virtualQueue = new VirtualQueue(host, request.getQueueName(), retentionPeriod);

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

        return new CreateQueueResult().withQueueUrl(virtualQueue.getID().getQueueUrl());
    }
    
    @Override
    public SendMessageResult sendMessage(SendMessageRequest request) {
        return VirtualQueueID.fromQueueUrl(request.getQueueUrl())
                .map(id -> id.sendMessage(amazonSqsToBeExtended, request))
                .orElseGet(() -> amazonSqsToBeExtended.sendMessage(request));
    }

    @Override
    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest request) {
        return getVirtualQueue(request.getQueueUrl())
                .map(virtualQueue -> virtualQueue.receiveMessage(request))
                .orElseGet(() -> amazonSqsToBeExtended.receiveMessage(request));
    }

    @Override
    public DeleteMessageResult deleteMessage(DeleteMessageRequest request) {
        return getVirtualQueue(request.getQueueUrl())
                .map(virtualQueue -> virtualQueue.deleteMessage(request))
                .orElseGet(() -> amazonSqsToBeExtended.deleteMessage(request));
    }

    @Override
    public DeleteQueueResult deleteQueue(DeleteQueueRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Deleting Virtual Queue is %s and Queue Name is %s", (virtualQueues.size() - 1), request.getQueueUrl()));
        }

        return getVirtualQueue(request.getQueueUrl())
                .map(virtualQueue -> virtualQueue.deleteQueue())
                .orElseGet(() -> amazonSqsToBeExtended.deleteQueue(request));
    }

    @Override
    public GetQueueAttributesResult getQueueAttributes(GetQueueAttributesRequest request) {
        return getVirtualQueue(request.getQueueUrl())
                .map(virtualQueue -> virtualQueue.getQueueAttributes(request))
                .orElseGet(() -> amazonSqsToBeExtended.getQueueAttributes(request));
    }
    
    @Override
    public SetQueueAttributesResult setQueueAttributes(SetQueueAttributesRequest request) {
        if (VirtualQueueID.fromQueueUrl(request.getQueueUrl()).isPresent()) {
            throw new IllegalArgumentException("Cannot change queue attributes of virtual queues after creation: " + request.getQueueUrl());
        } else {
            return amazonSqsToBeExtended.setQueueAttributes(request);
        }
    }

    @Override
    public TagQueueResult tagQueue(TagQueueRequest request) {
        return getVirtualQueue(request.getQueueUrl())
                .map(virtualQueue -> virtualQueue.tagQueue(request))
                .orElseGet(() -> amazonSqsToBeExtended.tagQueue(request));
    }
    
    @Override
    public UntagQueueResult untagQueue(UntagQueueRequest request) {
        return getVirtualQueue(request.getQueueUrl())
                .map(virtualQueue -> virtualQueue.untagQueue(request))
                .orElseGet(() -> amazonSqsToBeExtended.untagQueue(request));
    }
    
    @Override
    public ListQueueTagsResult listQueueTags(ListQueueTagsRequest request) {
        return getVirtualQueue(request.getQueueUrl())
                .map(virtualQueue -> virtualQueue.listQueueTags())
                .orElseGet(() -> amazonSqsToBeExtended.listQueueTags(request));
    }
    
    @Override
    public void shutdown() {
        hostQueues.values().parallelStream().forEach(HostQueue::shutdown);
        super.shutdown();
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
            String queueName = message.getMessageAttributes().get(VIRTUAL_QUEUE_NAME_ATTRIBUTE).getStringValue();
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
        
        public GetQueueAttributesResult getQueueAttributes(GetQueueAttributesRequest request) {
            List<String> attributeNames = request.getAttributeNames();
            boolean includeHostQueue = 
                    attributeNames.remove(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE) ||
                    attributeNames.contains("All");
            boolean includeRetentionPeriod = retentionPeriod.isPresent() && 
                    (attributeNames.contains(IDLE_QUEUE_RETENTION_PERIOD) ||
                     attributeNames.contains("All"));
            
            GetQueueAttributesRequest hostQueueRequest = new GetQueueAttributesRequest()
                    .withQueueUrl(hostQueue.queueUrl)
                    .withAttributeNames(attributeNames);
            GetQueueAttributesResult result = amazonSqsToBeExtended.getQueueAttributes(hostQueueRequest);
            if (includeHostQueue) {
                result.getAttributes().put(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE, hostQueue.queueUrl);
            }
            if (includeRetentionPeriod) {
                result.getAttributes().put(IDLE_QUEUE_RETENTION_PERIOD, retentionPeriod.get().toString());
            }
            return result;
        }
        
        public ReceiveMessageResult receiveMessage(ReceiveMessageRequest request) {
            heartbeat();
            try {
                try {
                    return receiveBuffer.receiveMessageAsync(request)
                                        .get(request.getWaitTimeSeconds(), TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    // Fall through to an empty receive
                } catch (ExecutionException e) {
                    throw (RuntimeException)e.getCause();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
    
                // Empty receive
                return new ReceiveMessageResult();
            } finally {
                heartbeat();
            }
        }
        
        public DeleteMessageResult deleteMessage(DeleteMessageRequest request) {
            heartbeat();
            DeleteMessageRequest virtualQueueResult = new DeleteMessageRequest()
                    .withQueueUrl(id.getHostQueueUrl())
                    .withReceiptHandle(request.getReceiptHandle());
            return amazonSqsToBeExtended.deleteMessage(virtualQueueResult);
        }
        
        public void heartbeat() {
            expireFuture.ifPresent(f -> f.cancel(false));
            expireFuture = retentionPeriod.map(period ->
                    executor.schedule(() -> AmazonSQSVirtualQueuesClient.this.deleteQueue(id.getQueueUrl()), period, TimeUnit.SECONDS));
        }
        
        public DeleteQueueResult deleteQueue() {
            virtualQueues.remove(id.getVirtualQueueName());
            receiveBuffer.shutdown();
            expireFuture.ifPresent(f -> f.cancel(false));
            return new DeleteQueueResult();
        }
        
        public TagQueueResult tagQueue(TagQueueRequest request) {
            tags.putAll(request.getTags());
            return new TagQueueResult();
        }
        
        public UntagQueueResult untagQueue(UntagQueueRequest request) {
            tags.keySet().removeAll(request.getTagKeys());
            return new UntagQueueResult();
        }
        
        public ListQueueTagsResult listQueueTags() {
            return new ListQueueTagsResult().withTags(tags);
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
        
        public SendMessageResult sendMessage(AmazonSQS sqs, SendMessageRequest request) {
            SendMessageRequest virtualQueueRequest = SQSQueueUtils.copyWithExtraAttributes(request, Collections.singletonMap(VIRTUAL_QUEUE_NAME_ATTRIBUTE, 
                    new MessageAttributeValue().withDataType("String").withStringValue(virtualQueueName)));
            virtualQueueRequest.setQueueUrl(hostQueueUrl);
            return sqs.sendMessage(virtualQueueRequest);
        }
    }
}
