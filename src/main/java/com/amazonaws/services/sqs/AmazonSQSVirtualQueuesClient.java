package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
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

// TODO-RS: Respect IdleQueueRetentionPeriodSeconds as well.
class AmazonSQSVirtualQueuesClient extends AbstractAmazonSQSClientWrapper {

    private static final Log LOG = LogFactory.getLog(AmazonSQSVirtualQueuesClient.class);

    public static final String VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE = "HostQueueUrl";
    private static final String VIRTUAL_QUEUE_NAME_ATTRIBUTE = "__AmazonSQSVirtualQueuesClient.QueueName";

    private static final BiConsumer<String, Message> DEFAULT_ORPHANED_MESSAGE_HANDLER = (queueName, message) -> {
        LOG.warn("Orphaned message sent to " + queueName + ": " + message.getMessageId());
    };

    private final ConcurrentMap<String, HostQueue> hostQueues = new ConcurrentHashMap<>();

    private final BiConsumer<String, Message> orphanedMessageHandler;

    public AmazonSQSVirtualQueuesClient(AmazonSQS amazonSqsToBeExtended) {
        this(amazonSqsToBeExtended, DEFAULT_ORPHANED_MESSAGE_HANDLER);
    }

    public AmazonSQSVirtualQueuesClient(AmazonSQS amazonSqsToBeExtended, BiConsumer<String, Message> orphanedMessageHandler) {
        super(amazonSqsToBeExtended);
        this.orphanedMessageHandler = orphanedMessageHandler;
    }

    @Override
    public CreateQueueResult createQueue(CreateQueueRequest request) {
        String hostQueueUrl = request.getAttributes().get(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);
        if (hostQueueUrl == null) {
            return amazonSqsToBeExtended.createQueue(request);
        }

        Set<String> otherAttributes = new HashSet<>(request.getAttributes().keySet());
        otherAttributes.remove(VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE);
        if (request.getAttributes().size() > 1) {
            throw new IllegalArgumentException("Virtual queues do not support setting these queue attributes independently of their host queues: " + otherAttributes);
        }

        HostQueue host = hostQueues.computeIfAbsent(hostQueueUrl, HostQueue::new);

        return new CreateQueueResult().withQueueUrl(host.createVirtualQueue(request.getQueueName()));
    }

    @Override
    public SendMessageResult sendMessage(SendMessageRequest request) {
        VirtualQueueID virtualQueue = VirtualQueueID.fromQueueUrl(request.getQueueUrl());
        if (virtualQueue != null) {
            SendMessageRequest virtualQueueRequest = SQSQueueUtils.copyWithExtraAttributes(request, Collections.singletonMap(VIRTUAL_QUEUE_NAME_ATTRIBUTE, 
                    new MessageAttributeValue().withDataType("String").withStringValue(virtualQueue.getVirtualQueueName())));
            virtualQueueRequest.setQueueUrl(virtualQueue.getHostQueueUrl());
            return amazonSqsToBeExtended.sendMessage(virtualQueueRequest);
        } else {
            return amazonSqsToBeExtended.sendMessage(request);
        }
    }

    @Override
    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest request) {
        VirtualQueueID virtualQueueID = VirtualQueueID.fromQueueUrl(request.getQueueUrl());
        if (virtualQueueID == null) {
            return amazonSqsToBeExtended.receiveMessage(request);
        }

        HostQueue host = hostQueues.get(virtualQueueID.getHostQueueUrl());
        if (host == null) {
            throw new QueueDoesNotExistException(request.getQueueUrl());
        }
        ReceiveQueueBuffer queue = host.getVirtualQueue(virtualQueueID.getVirtualQueueName());
        if (queue == null) {
            throw new QueueDoesNotExistException(request.getQueueUrl());
        }

        try {
            return queue.receiveMessageAsync(request)
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
    }

    @Override
    public DeleteMessageResult deleteMessage(DeleteMessageRequest request) {
        VirtualQueueID virtualQueueID = VirtualQueueID.fromQueueUrl(request.getQueueUrl());
        if (virtualQueueID == null) {
            return amazonSqsToBeExtended.deleteMessage(request);
        } else {
            // TODO-RS: Don't modify requests!
            request.withQueueUrl(virtualQueueID.getHostQueueUrl());
            return amazonSqsToBeExtended.deleteMessage(request);
        }
    }

    @Override
    public DeleteQueueResult deleteQueue(DeleteQueueRequest request) {
        VirtualQueueID virtualQueueID = VirtualQueueID.fromQueueUrl(request.getQueueUrl());
        if (virtualQueueID == null) {
            return amazonSqsToBeExtended.deleteQueue(request);
        } else {
            HostQueue hostQueue = hostQueues.get(virtualQueueID.getHostQueueUrl());
            hostQueue.deleteVirtualQueue(virtualQueueID.getVirtualQueueName());
            return new DeleteQueueResult();
        }
    }

    @Override
    public SetQueueAttributesResult setQueueAttributes(SetQueueAttributesRequest request) {
        VirtualQueueID virtualQueueID = VirtualQueueID.fromQueueUrl(request.getQueueUrl());
        if (virtualQueueID == null) {
            return amazonSqsToBeExtended.setQueueAttributes(request);
        } else {
            throw new IllegalArgumentException("Cannot change queue attributes of virtual queues after creation: " + request.getQueueUrl());
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

        // TODO-RS: Background tasks to delete virtual queues according to IdleQueueRetentionPeriodSeconds.
        // May want to use a LinkedHashMap with the accessOrder option to simplify
        // expiring queues with the same retention period (which will be the norm).
        // Should agree with the service team on the queue attribute name/semantics so it can later
        // be supported server-side as well.
        private final ConcurrentMap<String, ReceiveQueueBuffer> virtualQueues = new ConcurrentHashMap<>();

        public HostQueue(String queueUrl) {
            this.queueUrl = queueUrl;
            // Used to avoid 
            this.buffer = new ReceiveQueueBuffer(amazonSqsToBeExtended, queueUrl);
            this.consumer = new SQSMessageConsumer(AmazonSQSVirtualQueuesClient.this, queueUrl, this::dispatchMessage);
            this.consumer.start();
        }

        public String createVirtualQueue(String queueName) {
            virtualQueues.put(queueName, new ReceiveQueueBuffer(buffer));
            return new VirtualQueueID(queueUrl, queueName).getQueueUrl();
        }

        public ReceiveQueueBuffer getVirtualQueue(String queueName) {
            return virtualQueues.get(queueName);
        }

        public void deleteVirtualQueue(String queueName) {
            ReceiveQueueBuffer virtualQueue = virtualQueues.remove(queueName);
            if (virtualQueue != null) {
                virtualQueue.shutdown();
            }
        }

        private void dispatchMessage(Message message) {
            String queueName = message.getMessageAttributes().get(VIRTUAL_QUEUE_NAME_ATTRIBUTE).getStringValue();
            ReceiveQueueBuffer virtualQueue = virtualQueues.get(queueName);
            if (virtualQueue != null) {
                virtualQueue.deliverMessages(Collections.singletonList(message), queueUrl, null);
            } else {
                orphanedMessageHandler.accept(queueName, message);
            }
        }

        public void shutdown() {
            this.consumer.shutdown();
        }
    }

    private static class VirtualQueueID {

        private final String hostQueueUrl;
        private final String virtualQueueName;

        public static VirtualQueueID fromQueueUrl(String queueUrl) {
            int index = queueUrl.indexOf('#');
            if (index >= 0) {
                return new VirtualQueueID(queueUrl.substring(0, index), queueUrl.substring(index + 1));
            } else {
                return null;
            }
        }

        public VirtualQueueID(String hostQueueUrl, String virtualQueueName) {
            this.hostQueueUrl = hostQueueUrl;
            this.virtualQueueName = virtualQueueName;
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
    }
}
