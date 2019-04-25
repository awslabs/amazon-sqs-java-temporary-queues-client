package com.amazonaws.services.sqs;

public class AmazonSQSTemporaryQueuesClientBuilder {

    private AmazonSQSTemporaryQueuesClientBuilder() {
    }

    private String queuePrefix = "";

    public String getQueuePrefix() {
        return queuePrefix;
    }
    
    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }
    
    public AmazonSQSTemporaryQueuesClientBuilder withQueuePrefix(String queuePrefix) {
        setQueuePrefix(queuePrefix);
        return this;
    }

    /**
     * @return Create new instance of builder with all defaults set.
     */
    public static AmazonSQSTemporaryQueuesClientBuilder standard() {
        return new AmazonSQSTemporaryQueuesClientBuilder();
    }
    
    public static AmazonSQS defaultClient() {
        return standard().build();
    }
    
    public AmazonSQS build() {
        AmazonSQSRequesterClientBuilder requesterBuilder = AmazonSQSRequesterClientBuilder.standard()
                .withInternalQueuePrefix(queuePrefix);
        return AmazonSQSTemporaryQueuesClient.make(requesterBuilder).getWrappedClient();
    }
}
