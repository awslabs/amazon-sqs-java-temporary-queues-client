package com.amazonaws.services.sqs;

import java.util.Optional;

public class AmazonSQSResponderClientBuilder {

    private Optional<AmazonSQS> customSQS = Optional.empty();
    
    private String internalQueuePrefix = "__RequesterClientQueues__";
    
    private AmazonSQSResponderClientBuilder() {
    }
    
    public static AmazonSQSResponder defaultClient() {
        return standard().build();
    }
    
    public Optional<AmazonSQS> getAmazonSQS() {
        return customSQS;
    }
    
    public void setAmazonSQS(AmazonSQS sqs) {
        this.customSQS = Optional.of(sqs);
    }
    
    public AmazonSQSResponderClientBuilder withAmazonSQS(AmazonSQS sqs) {
        setAmazonSQS(sqs);
        return this;
    }

    public String getInternalQueuePrefix() {
        return internalQueuePrefix;
    }
    
    public void setInternalQueuePrefix(String internalQueuePrefix) {
        this.internalQueuePrefix = internalQueuePrefix;
    }
    
    public AmazonSQSResponderClientBuilder withInternalQueuePrefix(String internalQueuePrefix) {
        setInternalQueuePrefix(internalQueuePrefix);
        return this;
    }
    
    /**
     * @return Create new instance of builder with all defaults set.
     */
    public static AmazonSQSResponderClientBuilder standard() {
        return new AmazonSQSResponderClientBuilder();
    }
    
    public AmazonSQSResponder build() {
        AmazonSQS sqs = customSQS.orElseGet(AmazonSQSClientBuilder::defaultClient);
        AmazonSQS deleter = new AmazonSQSIdleQueueDeletingClient(sqs, internalQueuePrefix);
        AmazonSQS virtualQueuesClient = new AmazonSQSVirtualQueuesClient(deleter);
        return new AmazonSQSResponderClient(virtualQueuesClient);
    }
}
