package com.amazonaws.services.sqs.util;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.core.util.VersionInfo;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.AddPermissionRequest;
import software.amazon.awssdk.services.sqs.model.AddPermissionResponse;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ListDeadLetterSourceQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListDeadLetterSourceQueuesResponse;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsRequest;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsResponse;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.PurgeQueueResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.RemovePermissionRequest;
import software.amazon.awssdk.services.sqs.model.RemovePermissionResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.TagQueueRequest;
import software.amazon.awssdk.services.sqs.model.TagQueueResponse;
import software.amazon.awssdk.services.sqs.model.UntagQueueRequest;
import software.amazon.awssdk.services.sqs.model.UntagQueueResponse;

import java.util.Objects;

public class AbstractAmazonSQSClientWrapper implements SqsClient {
    protected final String USER_AGENT_VERSION = VersionInfo.SDK_VERSION;
    protected final String userAgentName;
    protected final SqsClient amazonSqsToBeExtended;

    public AbstractAmazonSQSClientWrapper(SqsClient amazonSqsToBeExtended) {
        this.amazonSqsToBeExtended = Objects.requireNonNull(amazonSqsToBeExtended);
        this.userAgentName = AbstractAmazonSQSClientWrapper.class.getSimpleName();
    }

    public AbstractAmazonSQSClientWrapper(SqsClient amazonSqsToBeExtended, String userAgentName) {
        this.amazonSqsToBeExtended = Objects.requireNonNull(amazonSqsToBeExtended);
        this.userAgentName = userAgentName;
    }

    @Override
    public AddPermissionResponse addPermission(AddPermissionRequest request) {
        return amazonSqsToBeExtended.addPermission(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public ChangeMessageVisibilityResponse changeMessageVisibility(ChangeMessageVisibilityRequest request) {
        return amazonSqsToBeExtended.changeMessageVisibility(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public ChangeMessageVisibilityBatchResponse changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest request) {
        return amazonSqsToBeExtended.changeMessageVisibilityBatch(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public CreateQueueResponse createQueue(CreateQueueRequest request) {
        return amazonSqsToBeExtended.createQueue(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public DeleteMessageResponse deleteMessage(DeleteMessageRequest request) {
        return amazonSqsToBeExtended.deleteMessage(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public DeleteQueueResponse deleteQueue(DeleteQueueRequest request) {
        return amazonSqsToBeExtended.deleteQueue(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public DeleteMessageBatchResponse deleteMessageBatch(DeleteMessageBatchRequest request) {
        return amazonSqsToBeExtended.deleteMessageBatch(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public GetQueueAttributesResponse getQueueAttributes(GetQueueAttributesRequest request) {
        return amazonSqsToBeExtended.getQueueAttributes(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public GetQueueUrlResponse getQueueUrl(GetQueueUrlRequest request) {
        return amazonSqsToBeExtended.getQueueUrl(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public ListDeadLetterSourceQueuesResponse listDeadLetterSourceQueues(ListDeadLetterSourceQueuesRequest request) {
        return amazonSqsToBeExtended.listDeadLetterSourceQueues(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public ListQueuesResponse listQueues(ListQueuesRequest request) {
        return amazonSqsToBeExtended.listQueues(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public ListQueueTagsResponse listQueueTags(ListQueueTagsRequest request) {
        return amazonSqsToBeExtended.listQueueTags(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public PurgeQueueResponse purgeQueue(PurgeQueueRequest request) {
        return amazonSqsToBeExtended.purgeQueue(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest request) {
        return amazonSqsToBeExtended.receiveMessage(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public RemovePermissionResponse removePermission(RemovePermissionRequest request) {
        return amazonSqsToBeExtended.removePermission(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public SendMessageResponse sendMessage(SendMessageRequest request) {
        return amazonSqsToBeExtended.sendMessage(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public SendMessageBatchResponse sendMessageBatch(SendMessageBatchRequest request) {
        return amazonSqsToBeExtended.sendMessageBatch(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public SetQueueAttributesResponse setQueueAttributes(SetQueueAttributesRequest request) {
        return amazonSqsToBeExtended.setQueueAttributes(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public TagQueueResponse tagQueue(TagQueueRequest request) {
        return amazonSqsToBeExtended.tagQueue(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public UntagQueueResponse untagQueue(UntagQueueRequest request) {
        return amazonSqsToBeExtended.untagQueue(appendUserAgent(request.toBuilder()).build());
    }

    @Override
    public String serviceName() {
        return amazonSqsToBeExtended.serviceName();
    }

    @Override
    public void close() {
        // By default do NOT close the wrapped client. It may or may not
        // make sense to based on whether the wrapper owns the wrapped client or not.
    }

    private <T extends AwsRequest.Builder> T appendUserAgent(final T builder) {
        return (T) builder
                .overrideConfiguration(
                        AwsRequestOverrideConfiguration.builder()
                                .addApiName(ApiName.builder().name(userAgentName)
                                        .version(USER_AGENT_VERSION).build())
                                .build());
    }
}
