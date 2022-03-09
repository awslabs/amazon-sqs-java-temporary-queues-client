package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

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
        return new MessageContent(message.body(), message.messageAttributes());
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
        return SendMessageRequest.builder().messageBody(messageBody).messageAttributes(new HashMap<>(messageAttributes)).build();
    }

    public SendMessageBatchRequestEntry toSendMessageBatchRequestEntry() {
        return SendMessageBatchRequestEntry.builder().messageBody(messageBody).messageAttributes(new HashMap<>(messageAttributes)).build();
    }
    
    public Message toMessage() {
        return Message.builder().body(messageBody).messageAttributes(new HashMap<>(messageAttributes)).build();
    }
}
