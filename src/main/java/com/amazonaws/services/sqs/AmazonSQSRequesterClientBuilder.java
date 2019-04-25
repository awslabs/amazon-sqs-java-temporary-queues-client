package com.amazonaws.services.sqs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AmazonSQSRequesterClientBuilder {
    
    private Optional<AmazonSQS> customSQS = Optional.empty();
    
    private String internalQueuePrefix = "__RequesterClientQueues__";
    
    private Map<String, String> queueAttributes = Collections.emptyMap();
    
    private AmazonSQSRequesterClientBuilder() {
    }

    /**
     * @return Create new instance of builder with all defaults set.
     */
    public static AmazonSQSRequesterClientBuilder standard() {
        return new AmazonSQSRequesterClientBuilder();
    }
    
    public static AmazonSQSRequester defaultClient() {
        return standard().build();
    }
    
    public Optional<AmazonSQS> getAmazonSQS() {
        return customSQS;
    }
    
    public void setAmazonSQS(AmazonSQS sqs) {
        this.customSQS = Optional.of(sqs);
    }
    
    public AmazonSQSRequesterClientBuilder withAmazonSQS(AmazonSQS sqs) {
        setAmazonSQS(sqs);
        return this;
    }

    public String getInternalQueuePrefix() {
        return internalQueuePrefix;
    }
    
    public void setInternalQueuePrefix(String internalQueuePrefix) {
        this.internalQueuePrefix = internalQueuePrefix;
    }
    
    public AmazonSQSRequesterClientBuilder withInternalQueuePrefix(String internalQueuePrefix) {
        setInternalQueuePrefix(internalQueuePrefix);
        return this;
    }
    
    public Map<String, String> getQueueAttributes() {
        return Collections.unmodifiableMap(queueAttributes);
    }
    
    public void setQueueAttributes(Map<String, String> queueAttributes) {
        this.queueAttributes = new HashMap<>(queueAttributes);
    }
    
    public AmazonSQSRequesterClientBuilder withQueueAttributes(Map<String, String> queueAttributes) {
        setQueueAttributes(queueAttributes);
        return this;
    }

    public AmazonSQSRequester build() {
        return AmazonSQSTemporaryQueuesClient.make(this).getRequester();
    }
}
