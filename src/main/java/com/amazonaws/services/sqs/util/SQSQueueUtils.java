package com.amazonaws.services.sqs.util;

import static com.amazonaws.services.sqs.executors.ExecutorUtils.acceptIntOn;
import static com.amazonaws.services.sqs.executors.ExecutorUtils.acceptOn;
import static com.amazonaws.services.sqs.executors.ExecutorUtils.applyIntOn;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSQueueUtils {

    public static final String ATTRIBUTE_NAMES_ALL = "All";

    public static final String MESSAGE_ATTRIBUTE_TYPE_STRING = "String";
    public static final String MESSAGE_ATTRIBUTE_TYPE_BOOLEAN = "String.boolean";
    public static final String MESSAGE_ATTRIBUTE_TYPE_LONG = "Number.long";

    public static final int SQS_LIST_QUEUES_LIMIT = 1000;

    private SQSQueueUtils() {
        // Never instantiated
    }

    public static MessageAttributeValue stringMessageAttributeValue(String value) {
        return new MessageAttributeValue().withDataType(MESSAGE_ATTRIBUTE_TYPE_STRING)
                .withStringValue(value);
    }

    public static MessageAttributeValue longMessageAttributeValue(long value) {
        return new MessageAttributeValue().withDataType(MESSAGE_ATTRIBUTE_TYPE_LONG)
                .withStringValue(Long.toString(value));
    }

    public static MessageAttributeValue booleanMessageAttributeValue(boolean value) {
        return new MessageAttributeValue().withDataType(MESSAGE_ATTRIBUTE_TYPE_BOOLEAN)
                .withStringValue(Boolean.toString(value));
    }

    public static Optional<String> getStringMessageAttributeValue(Map<String, MessageAttributeValue> messageAttributes, String key) {
        return Optional.ofNullable(messageAttributes.get(key))
                .filter(value -> MESSAGE_ATTRIBUTE_TYPE_STRING.equals(value.getDataType()))
                .map(MessageAttributeValue::getStringValue);
    }

    public static Optional<Long> getLongMessageAttributeValue(Map<String, MessageAttributeValue> messageAttributes, String key) {
        return Optional.ofNullable(messageAttributes.get(key))
                .filter(value -> MESSAGE_ATTRIBUTE_TYPE_LONG.equals(value.getDataType()))
                .map(MessageAttributeValue::getStringValue)
                .map(Long::parseLong);
    }

    public static boolean getBooleanMessageAttributeValue(Map<String, MessageAttributeValue> messageAttributes, String key) {
        return Optional.ofNullable(messageAttributes.get(key))
                .filter(value -> MESSAGE_ATTRIBUTE_TYPE_BOOLEAN.equals(value.getDataType()))
                .map(MessageAttributeValue::getStringValue)
                .map(Boolean::parseBoolean).orElse(false);
    }

    // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html
    private static final String VALID_QUEUE_NAME_CHARACTERS;
    static {
        StringBuilder builder = new StringBuilder();
        IntStream.rangeClosed('a', 'z').forEach(i -> builder.append((char)i));
        IntStream.rangeClosed('A', 'Z').forEach(i -> builder.append((char)i));
        IntStream.rangeClosed('0', '9').forEach(i -> builder.append((char)i));
        builder.append('-');
        builder.append('_');
        VALID_QUEUE_NAME_CHARACTERS = builder.toString();
    }

    public static boolean isQueueEmpty(AmazonSQS sqs, String queueUrl) {
        QueueAttributeName[] messageCountAttrs = {
                QueueAttributeName.ApproximateNumberOfMessages,
                QueueAttributeName.ApproximateNumberOfMessagesDelayed,
                QueueAttributeName.ApproximateNumberOfMessagesNotVisible
        };

        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .withAttributeNames(messageCountAttrs);
        GetQueueAttributesResult result = sqs.getQueueAttributes(getQueueAttributesRequest);
        Map<String, String> attrValues = result.getAttributes();
        return Stream.of(messageCountAttrs).allMatch(attr ->
                Long.parseLong(attrValues.get(attr.name())) == 0);
    }

    public static boolean awaitWithPolling(long period, long timeout, TimeUnit unit, Supplier<Boolean> test) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (!test.get()) {
            if (deadlineNanos < System.nanoTime()) {
                return false;
            }
            // TODO-RS: Use a ScheduledExecutorService instead of sleeping?
            unit.sleep(period);
        }
        return true;
    }

    public static boolean awaitEmptyQueue(AmazonSQS sqs, String queueUrl, long timeout, TimeUnit unit) throws InterruptedException {
        // There's no way to be directly notified unfortunately.
        return awaitWithPolling(unit.convert(2, TimeUnit.SECONDS), timeout, unit, () -> isQueueEmpty(sqs, queueUrl));
    }

    public static void forEachQueue(ExecutorService executor, AmazonSQS sqs, String prefix, int limit, Consumer<String> action) {
        List<String> queueUrls = sqs.listQueues(prefix).getQueueUrls();
        if (queueUrls.size() >= limit) {
            // Manually work around the 1000 queue limit by forking for each
            // possible next character. Yes this is exponential with a factor of
            // 64, but we only fork when the results are more than 1000.
            VALID_QUEUE_NAME_CHARACTERS
                    .chars()
                    .parallel()
                    .forEach(acceptIntOn(executor, c ->
            forEachQueue(executor, sqs, prefix + (char)c, limit, action)));
        } else {
            queueUrls.forEach(acceptOn(executor, action));
        }
    }

    public static List<String> listQueues(ExecutorService executor, Function<String, List<String>> lister, String prefix, int limit) {
        List<String> queueUrls = lister.apply(prefix);
        if (queueUrls.size() >= limit) {
            // Manually work around the 1000 queue limit by forking for each
            // possible next character. Yes this is exponential with a factor of
            // 64, but we only fork when the results are more than 1000.
            return VALID_QUEUE_NAME_CHARACTERS
                    .chars()
                    .parallel()
                    .mapToObj(applyIntOn(executor, c -> listQueues(executor, lister, prefix + (char)c, limit)))
                    .map(List::stream)
                    .flatMap(Function.identity())
                    .collect(Collectors.toList());
        } else {
            return queueUrls;
        }
    }

    public static CreateQueueRequest copyWithExtraAttributes(CreateQueueRequest request, Map<String, String> extraAttrs) {
        Map<String, String> newAttributes = new HashMap<>(request.getAttributes());
        newAttributes.putAll(extraAttrs);

        // Clone to create a shallow copy that includes the superclass properties.
        return request.clone()
                .withQueueName(request.getQueueName())
                .withAttributes(newAttributes);
    }

    public static SendMessageRequest copyWithExtraAttributes(SendMessageRequest request, Map<String, MessageAttributeValue> extraAttrs) {
        Map<String, MessageAttributeValue> newAttributes = new HashMap<>(request.getMessageAttributes());
        newAttributes.putAll(extraAttrs);

        // Clone to create a shallow copy that includes the superclass properties.
        return request.clone()
                .withQueueUrl(request.getQueueUrl())
                .withMessageBody(request.getMessageBody())
                .withMessageAttributes(newAttributes)
                .withDelaySeconds(request.getDelaySeconds());
    }
}
