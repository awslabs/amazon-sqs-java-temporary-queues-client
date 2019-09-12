package com.amazonaws.services.sqs.util;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class SQSQueueUtilsTest {

    /**
     * This test fails if a new member is added to SendMessageRequest class.
     *
     * This acts as a remainder to notify us to update the
     * {@link SQSQueueUtils#copyWithExtraAttributes(SendMessageRequest, Map)} method when a
     * member is added to {@link SendMessageRequest}.
     */
    @Test
    public void sendMessageRequestCopyWithExtraAttributes() throws IllegalAccessException {

        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl("queueUrl")
                .withMessageBody("messageBody")
                .withDelaySeconds(5)
                .withMessageAttributes(Collections.emptyMap())
                .withMessageDeduplicationId("dedup")
                .withMessageGroupId("groupId");

        SendMessageRequest sendMessageRequestCopy =
                SQSQueueUtils.copyWithExtraAttributes(sendMessageRequest, Collections.emptyMap());

        assertNoFieldsAreNull(SendMessageRequest.class, sendMessageRequestCopy);
    }

    /**
     * This test fails if a new member is added to CreateQueueRequest class.
     *
     * This acts as a remainder to notify us to update the
     * {@link SQSQueueUtils#copyWithExtraAttributes(CreateQueueRequest, Map)} method when a
     * member is added to {@link CreateQueueRequest}.
     */
    @Test
    public void createQueueRequestCopyWithExtraAttributes() throws IllegalAccessException {

        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName("queueName")
                .withAttributes(Collections.emptyMap());

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
