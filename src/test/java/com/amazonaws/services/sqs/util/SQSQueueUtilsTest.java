package com.amazonaws.services.sqs.util;

import org.junit.Test;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class SQSQueueUtilsTest {

    /**
     * This test fails if a new member is added to SendMessageRequest class.
     *
     * This acts as a reminder to notify us to update the
     * {@link SQSQueueUtils#copyWithExtraAttributes(SendMessageRequest, Map)} method when a
     * member is added to {@link SendMessageRequest}.
     */
    @Test
    public void sendMessageRequestCopyWithExtraAttributes() throws IllegalAccessException {

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl("queueUrl")
                .messageBody("messageBody")
                .delaySeconds(5)
                .messageAttributes(Collections.emptyMap())
                .messageDeduplicationId("dedup")
                .messageGroupId("groupId")
                .messageSystemAttributes(Collections.emptyMap()).build();

        SendMessageRequest sendMessageRequestCopy =
                SQSQueueUtils.copyWithExtraAttributes(sendMessageRequest, Collections.emptyMap());

        assertNoFieldsAreNull(SendMessageRequest.class, sendMessageRequestCopy);
    }

    /**
     * This test fails if a new member is added to CreateQueueRequest class.
     *
     * This acts as a reminder to notify us to update the
     * {@link SQSQueueUtils#copyWithExtraAttributes(CreateQueueRequest, Map)} method when a
     * member is added to {@link CreateQueueRequest}.
     */
    @Test
    public void createQueueRequestCopyWithExtraAttributes() throws IllegalAccessException {

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName("queueName")
                .attributesWithStrings(Collections.emptyMap())
                .tags(Collections.emptyMap()).build();

        CreateQueueRequest createQueueRequestCopy =
                SQSQueueUtils.copyWithExtraAttributes(createQueueRequest, Collections.emptyMap());

        assertNoFieldsAreNull(CreateQueueRequest.class, createQueueRequestCopy);
    }

    private <T> void assertNoFieldsAreNull(Class<T> klass, T instance) throws IllegalAccessException {
        for (Field field : klass.getDeclaredFields()) {
            field.setAccessible(true);

            Object value = field.get(instance);

            // We don't have primitives in our models. So doing a NotNull check should be enough
            assertNotNull("Non-primitive fields should not be null, but " + field.getName() + " was null", value);
        }
    }
}
