package com.amazonaws.services.sqs.util;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.monitoring.ApiCallAttemptMonitoringEvent;
import com.amazonaws.monitoring.ApiCallMonitoringEvent;
import com.amazonaws.monitoring.MonitoringEvent;
import com.amazonaws.monitoring.MonitoringListener;
import com.amazonaws.services.sqs.AbstractAmazonSQS;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.AddPermissionResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityResult;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesRequest;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesResult;
import com.amazonaws.services.sqs.model.ListQueueTagsRequest;
import com.amazonaws.services.sqs.model.ListQueueTagsResult;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.PurgeQueueResult;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.RemovePermissionRequest;
import com.amazonaws.services.sqs.model.RemovePermissionResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesResult;
import com.amazonaws.services.sqs.model.TagQueueRequest;
import com.amazonaws.services.sqs.model.TagQueueResult;
import com.amazonaws.services.sqs.model.UntagQueueRequest;
import com.amazonaws.services.sqs.model.UntagQueueResult;
import com.amazonaws.util.VersionInfoUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class AbstractAmazonSQSClientWrapper extends AbstractAmazonSQS {

    private static final Log LOG = LogFactory.getLog(AbstractAmazonSQSClientWrapper.class);

    protected final AmazonSQS amazonSqsToBeExtended;
    // TODO-RS: Use RequestHandler2 instead!
    protected final String userAgent;
    protected final Collection<MonitoringListener> monitoringListeners;

    public AbstractAmazonSQSClientWrapper(AmazonSQS amazonSqsToBeExtended) {
        this.amazonSqsToBeExtended = Objects.requireNonNull(amazonSqsToBeExtended);
        this.userAgent = getClass().getSimpleName() + "/" + VersionInfoUtils.getVersion();
        this.monitoringListeners = extractListeners(amazonSqsToBeExtended);
    }

    public AbstractAmazonSQSClientWrapper(AmazonSQS amazonSqsToBeExtended, String userAgent) {
        this.amazonSqsToBeExtended = Objects.requireNonNull(amazonSqsToBeExtended);
        this.userAgent = userAgent;
        this.monitoringListeners = extractListeners(amazonSqsToBeExtended);
    }

    private Collection<MonitoringListener> extractListeners(AmazonSQS sqs) {
        if (sqs instanceof AmazonSQS) {
            return ((AmazonWebServiceClient)sqs).getMonitoringListeners();
        } else if (sqs instanceof AbstractAmazonSQSClientWrapper) {
            return ((AbstractAmazonSQSClientWrapper)sqs).getMonitoringListeners();
        } else {
            throw new IllegalArgumentException(("Couldn't extract monitoring listeners from " + sqs));
        }
    }

    public Collection<MonitoringListener> getMonitoringListeners() {
        return monitoringListeners;
    }

    protected <T> T monitor(MonitoredCall call, Supplier<T> supplier) {
        long startNanos = System.nanoTime();
        long latencyNanos;
        Exception thrown = null;
        try {
            T result = supplier.get();
            latencyNanos = System.nanoTime() - startNanos;
            ApiCallMonitoringEvent event = new ApiCallMonitoringEvent()
                    .withApi(call.name())
                    .withLatency(TimeUnit.NANOSECONDS.toMillis(latencyNanos));
            emitEvent(event);
            return result;
        } catch (Exception e) {
            thrown = e;
            throw e;
        } finally {
            latencyNanos = System.nanoTime() - startNanos;
            ApiCallAttemptMonitoringEvent event = new ApiCallAttemptMonitoringEvent()
                    .withApi(call.name())
                    .withAttemptLatency(TimeUnit.NANOSECONDS.toMillis(latencyNanos));
            if (thrown != null) {
                event.withSdkException(thrown.getClass().getName());
            }
            emitEvent(event);
        }
    }

    private void emitEvent(MonitoringEvent event) {
        for (MonitoringListener monitoringListener : monitoringListeners) {
            try {
                monitoringListener.handleEvent(event);
            } catch (Exception exception) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("MonitoringListener: %s failed to handle event", monitoringListener.toString()),
                            exception);
                }
            }
        }
    }

    @Override
    public AddPermissionResult addPermission(AddPermissionRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.addPermission(request);
    }

    @Override
    public ChangeMessageVisibilityResult changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.changeMessageVisibility(request);
    }

    @Override
    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.changeMessageVisibilityBatch(request);
    }

    @Override
    public CreateQueueResult createQueue(CreateQueueRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.createQueue(request);
    }

    @Override
    public DeleteMessageResult deleteMessage(DeleteMessageRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.deleteMessage(request);
    }

    @Override
    public DeleteQueueResult deleteQueue(DeleteQueueRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.deleteQueue(request);
    }

    @Override
    public DeleteMessageBatchResult deleteMessageBatch(DeleteMessageBatchRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.deleteMessageBatch(request);
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        return amazonSqsToBeExtended.getCachedResponseMetadata(request);
    }

    @Override
    public GetQueueAttributesResult getQueueAttributes(GetQueueAttributesRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.getQueueAttributes(request);
    }

    @Override
    public GetQueueUrlResult getQueueUrl(GetQueueUrlRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.getQueueUrl(request);
    }

    @Override
    public ListDeadLetterSourceQueuesResult listDeadLetterSourceQueues(ListDeadLetterSourceQueuesRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.listDeadLetterSourceQueues(request);
    }

    @Override
    public ListQueuesResult listQueues(ListQueuesRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.listQueues(request);
    }

    @Override
    public ListQueueTagsResult listQueueTags(ListQueueTagsRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.listQueueTags(request);
    }

    @Override
    public PurgeQueueResult purgeQueue(PurgeQueueRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.purgeQueue(request);
    }

    @Override
    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.receiveMessage(request);
    }

    @Override
    public RemovePermissionResult removePermission(RemovePermissionRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.removePermission(request);
    }

    @Override
    public SendMessageResult sendMessage(SendMessageRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.sendMessage(request);
    }

    @Override
    public SendMessageBatchResult sendMessageBatch(SendMessageBatchRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.sendMessageBatch(request);
    }

    @Override
    @Deprecated
    public void setEndpoint(String endpoint) {
        amazonSqsToBeExtended.setEndpoint(endpoint);
    }

    @Override
    public void shutdown() {
        // By default do NOT shutdown the wrapped client. It may or may not
        // make sense to based on whether the wrapper owns the wrapped client or not.
    }

    @Override
    public SetQueueAttributesResult setQueueAttributes(SetQueueAttributesRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.setQueueAttributes(request);
    }

    @Override
    public TagQueueResult tagQueue(TagQueueRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.tagQueue(request);
    }

    @Override
    public UntagQueueResult untagQueue(UntagQueueRequest request) {
        request.getRequestClientOptions().appendUserAgent(userAgent);
        return amazonSqsToBeExtended.untagQueue(request);
    }
}
