package com.amazonaws.services.sqs.util;

import static com.amazonaws.services.sqs.executors.ExecutorUtils.acceptIntOn;
import static com.amazonaws.services.sqs.executors.ExecutorUtils.acceptOn;
import static com.amazonaws.services.sqs.executors.ExecutorUtils.applyIntOn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.ListQueueTagsRequest;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SQSQueueUtils {
    private static final Log LOG = LogFactory.getLog(SQSQueueUtils.class);

    public static final String ATTRIBUTE_NAMES_ALL = "All";

    public static final String MESSAGE_ATTRIBUTE_TYPE_STRING = "String";
    public static final String MESSAGE_ATTRIBUTE_TYPE_BOOLEAN = "String.boolean";
    public static final String MESSAGE_ATTRIBUTE_TYPE_LONG = "Number.long";

    public static final int SQS_LIST_QUEUES_LIMIT = 1000;

    public static final Consumer<Exception> DEFAULT_EXCEPTION_HANDLER = e -> {
        LOG.error("Unexpected exception", e);
    };
    
    private SQSQueueUtils() {
        // Never instantiated
    }

    public static MessageAttributeValue stringMessageAttributeValue(String value) {
        return MessageAttributeValue.builder().dataType(MESSAGE_ATTRIBUTE_TYPE_STRING)
                .stringValue(value).build();
    }

    public static MessageAttributeValue longMessageAttributeValue(long value) {
        return MessageAttributeValue.builder().dataType(MESSAGE_ATTRIBUTE_TYPE_LONG)
                .stringValue(Long.toString(value)).build();
    }

    public static MessageAttributeValue booleanMessageAttributeValue(boolean value) {
        return MessageAttributeValue.builder().dataType(MESSAGE_ATTRIBUTE_TYPE_BOOLEAN)
                .stringValue(Boolean.toString(value)).build();
    }

    public static Optional<String> getStringMessageAttributeValue(Map<String, MessageAttributeValue> messageAttributes, String key) {
        return Optional.ofNullable(messageAttributes.get(key))
                .filter(value -> MESSAGE_ATTRIBUTE_TYPE_STRING.equals(value.dataType()))
                .map(MessageAttributeValue::stringValue);
    }

    public static Optional<Long> getLongMessageAttributeValue(Map<String, MessageAttributeValue> messageAttributes, String key) {
        return Optional.ofNullable(messageAttributes.get(key))
                .filter(value -> MESSAGE_ATTRIBUTE_TYPE_LONG.equals(value.dataType()))
                .map(MessageAttributeValue::stringValue)
                .map(Long::parseLong);
    }

    public static boolean getBooleanMessageAttributeValue(Map<String, MessageAttributeValue> messageAttributes, String key) {
        return Optional.ofNullable(messageAttributes.get(key))
                .filter(value -> MESSAGE_ATTRIBUTE_TYPE_BOOLEAN.equals(value.dataType()))
                .map(MessageAttributeValue::stringValue)
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

    public static boolean isQueueEmpty(SqsClient sqs, String queueUrl) {
        String[] messageCountAttrs = {
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES.toString(),
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED.toString(),
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE.toString()
        };

        GetQueueAttributesRequest.Builder getQueueAttributesBuilder = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNamesWithStrings(messageCountAttrs);
        GetQueueAttributesResponse result = sqs.getQueueAttributes(getQueueAttributesBuilder.build());
        Map<String, String> attrValues = result.attributesAsStrings();
        return Stream.of(messageCountAttrs).allMatch(attr ->
                Long.parseLong(attrValues.get(attr)) == 0);
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

    public static boolean awaitEmptyQueue(SqsClient sqs, String queueUrl, long timeout, TimeUnit unit) throws InterruptedException {
        // There's no way to be directly notified unfortunately.
        return awaitWithPolling(unit.convert(2, TimeUnit.SECONDS), timeout, unit, () -> isQueueEmpty(sqs, queueUrl));
    }

    public static boolean doesQueueExist(SqsClient sqs, String queueUrl) {
        try {
            ListQueueTagsRequest.Builder builder = ListQueueTagsRequest.builder().queueUrl(queueUrl);
            sqs.listQueueTags(builder.build());
            return true;
        } catch (QueueDoesNotExistException e) {
            return false;
        }
    }
    
    public static boolean awaitQueueCreated(SqsClient sqs, String queueUrl, long timeout, TimeUnit unit) throws InterruptedException {
        return awaitWithPolling(unit.convert(2, TimeUnit.SECONDS), timeout, unit, () -> doesQueueExist(sqs, queueUrl));
    }

    public static boolean awaitQueueDeleted(SqsClient sqs, String queueUrl, long timeout, TimeUnit unit) throws InterruptedException {
        return awaitWithPolling(unit.convert(2, TimeUnit.SECONDS), timeout, unit, () -> !doesQueueExist(sqs, queueUrl));
    }

    public static void forEachQueue(ExecutorService executor, Function<String, List<String>> lister, String prefix, int limit, Consumer<String> action) {
        List<String> queueUrls = lister.apply(prefix);
        if (queueUrls.size() >= limit) {
            // Manually work around the 1000 queue limit by forking for each
            // possible next character. Yes this is exponential with a factor of
            // 64, but we only fork when the results are more than 1000.
            VALID_QUEUE_NAME_CHARACTERS
                    .chars()
                    .parallel()
                    .forEach(acceptIntOn(executor, c ->
            forEachQueue(executor, lister, prefix + (char)c, limit, action)));
        } else {
            queueUrls.forEach(acceptOn(executor, action));
        }
    }

    public static List<String> listQueues(ExecutorService executor, Function<String, List<String>> lister, String prefix, int limit) {
        List<String> queueUrls = new ArrayList<>(lister.apply(prefix));

        if (queueUrls.size() >= limit) {
            // Manually work around the 1000 queue limit by forking for each
            // possible next character. Yes this is exponential with a factor of
            // 64, but we only fork when the results are more than 1000.
            List<List<String>> collect = VALID_QUEUE_NAME_CHARACTERS.chars().parallel().mapToObj(applyIntOn(executor, c -> listQueues(executor, lister, prefix + (char) c, limit))).collect(Collectors.toList());
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
        Map<String, String> newAttributes = new HashMap<>(request.attributesAsStrings());
        newAttributes.putAll(extraAttrs);

        // Create a shallow copy that includes the superclass properties.
        return request.toBuilder().copy()
                .queueName(request.queueName())
                .attributesWithStrings(newAttributes).build();
    }

    public static SendMessageRequest copyWithExtraAttributes(SendMessageRequest request, Map<String, MessageAttributeValue> extraAttrs) {
        Map<String, MessageAttributeValue> newAttributes = new HashMap<>(request.messageAttributes());
        newAttributes.putAll(extraAttrs);

        // Create a shallow copy that includes the superclass properties.
        return request.toBuilder().copy()
                .queueUrl(request.queueUrl())
                .messageBody(request.messageBody())
                .messageAttributes(newAttributes)
                .delaySeconds(request.delaySeconds()).build();
    }
    
    /**
     * this method carefully waits for futures. If waiting throws, it converts the exceptions to the
     * exceptions that SQS clients expect. This is what we use to turn asynchronous calls into
     * synchronous ones.
     */
    // TODO-RS: Copied from QueueBuffer in the buffered asynchronous client
    public static <V> V waitForFuture(Future<V> future) {
        V toReturn = null;
        try {
            toReturn = future.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw SdkClientException.create(
                    "Thread interrupted while waiting for execution result", ie);
        } catch (ExecutionException ee) {
            // if the cause of the execution exception is an SQS exception, extract it
            // and throw the extracted exception to the clients
            // otherwise, wrap ee in an SQS exception and throw that.
            Throwable cause = ee.getCause();

            if (cause instanceof SdkClientException) {
                throw (SdkClientException) cause;
            }

            throw SdkClientException.create(
                    "Caught an exception while waiting for request to complete...", ee);
        }

        return toReturn;
    }
}
