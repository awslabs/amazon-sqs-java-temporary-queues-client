package com.amazonaws.services.sqs.responsesapi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class MessageContent {

    private final String messageBody;
    private final Map<String, MessageAttributeValue> messageAttributes;

    public MessageContent(String messageBody) {
        this.messageBody = Objects.requireNonNull(messageBody);
        this.messageAttributes = new HashMap<>();
    }

    public MessageContent(String messageBody, Map<String, MessageAttributeValue> attributes) {
        this.messageBody = Objects.requireNonNull(messageBody);
        this.messageAttributes = new HashMap<>(attributes);
    }

    public static MessageContent fromMessage(Message message) {
        return new MessageContent(message.getBody(), message.getMessageAttributes());
    }

    public String getMessageBody() {
        return messageBody;
    }

    public Map<String, MessageAttributeValue> getMessageAttributes() {
        return Collections.unmodifiableMap(messageAttributes);
    }

    public void setMessageAttributesEntry(String key, MessageAttributeValue value) {
        messageAttributes.put(key, value);
    }

    public SendMessageRequest toSendMessageRequest() {
        return new SendMessageRequest().withMessageBody(messageBody).withMessageAttributes(new HashMap<>(messageAttributes));
    }

    public SendMessageBatchRequestEntry toSendMessageBatchRequestEntry() {
        return new SendMessageBatchRequestEntry().withMessageBody(messageBody).withMessageAttributes(new HashMap<>(messageAttributes));
    }
}
