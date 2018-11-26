package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import com.amazonaws.services.sqs.util.AbstractAmazonSQSClientWrapper;
import com.amazonaws.services.sqs.util.ReceiveQueueBuffer;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSQueueUtils;

class AmazonSQSVirtualQueuesClient extends AbstractAmazonSQSClientWrapper {

    private static final Log LOG = LogFactory.getLog(AmazonSQSVirtualQueuesClient.class);

    public static final String VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE = "HostQueueUrl";
    private static final String VIRTUAL_QUEUE_NAME_ATTRIBUTE = "__AmazonSQSVirtualQueuesClient.QueueName";

    private static final BiConsumer<String, Message> DEFAULT_ORPHANED_MESSAGE_HANDLER = (queueName, message) -> {
        LOG.warn("Orphaned message sent to " + queueName + ": " + message.getMessageId());
    };

    private final ConcurrentMap<String, HostQueue> hostQueues = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, VirtualQueue> virtualQueues = new ConcurrentHashMap<>();

    private final BiConsumer<String, Message> orphanedMessageHandler;
    
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public AmazonSQSVirtualQueuesClient(AmazonSQS amazonSqsToBeExtended) {
        this(amazonSqsToBeExtended, DEFAULT_ORPHANED_MESSAGE_HANDLER);
    }

    public AmazonSQSVirtualQueuesClient(AmazonSQS amazonSqsToBeExtended, BiConsumer<String, Message> orphanedMessageHandler) {
        super(amazonSqsToBeExtended);
        this.orphanedMessageHandler = orphanedMessageHandler;
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
            throw new IllegalArgumentException("Virtual queues do not support setting these queue attributes independently of their host queues: " + attributes.keySet());
        }

        HostQueue host = hostQueues.computeIfAbsent(hostQueueUrl, HostQueue::new);
        VirtualQueue virtualQueue = new VirtualQueue(host, request.getQueueName(), retentionPeriod);
        virtualQueues.put(virtualQueue.getID().getVirtualQueueName(), virtualQueue);
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
        return getVirtualQueue(request.getQueueUrl())
                .map(virtualQueue -> virtualQueue.deleteQueue(request))
                .orElseGet(() -> amazonSqsToBeExtended.deleteQueue(request));
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
    public void shutdown() {
        hostQueues.values().forEach(HostQueue::shutdown);
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
            this.buffer = new ReceiveQueueBuffer(amazonSqsToBeExtended, queueUrl);
            this.consumer = new SQSMessageConsumer(AmazonSQSVirtualQueuesClient.this, queueUrl, this::dispatchMessage);
            this.consumer.start();
        }

        private void dispatchMessage(Message message) {
            String queueName = message.getMessageAttributes().get(VIRTUAL_QUEUE_NAME_ATTRIBUTE).getStringValue();
            VirtualQueue virtualQueue = virtualQueues.get(queueName);
            if (virtualQueue != null) {
                virtualQueue.receiveBuffer.deliverMessages(Collections.singletonList(message), queueUrl, null);
            } else {
                orphanedMessageHandler.accept(queueName, message);
            }
        }

        public void shutdown() {
            this.buffer.shutdown();
            this.consumer.shutdown();
        }
    }

    private class VirtualQueue {
        
        private final VirtualQueueID id;
        private final ReceiveQueueBuffer receiveBuffer;
        private final Optional<Long> retentionPeriod;
        private Optional<ScheduledFuture<?>> expireFuture;
        
        public VirtualQueue(HostQueue hostQueue, String queueName, Optional<Long> retentionPeriod) {
            this.id = new VirtualQueueID(hostQueue.queueUrl, queueName);
            this.receiveBuffer = new ReceiveQueueBuffer(hostQueue.buffer);
            this.retentionPeriod = retentionPeriod;
            this.expireFuture = Optional.empty();
            heartbeat();
        }
        
        public VirtualQueueID getID() {
            return id;
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
        
        public DeleteQueueResult deleteQueue(DeleteQueueRequest request) {
            virtualQueues.remove(id.getVirtualQueueName());
            receiveBuffer.shutdown();
            expireFuture.ifPresent(f -> f.cancel(false));
            return new DeleteQueueResult();
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
