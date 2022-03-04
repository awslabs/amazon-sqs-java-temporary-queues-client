package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.util.Constants;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.Optional;

public class AmazonSQSResponderClientBuilder {

    private Optional<SqsClient> customSQS = Optional.empty();
    
    private String internalQueuePrefix = "__RequesterClientQueues__";
    private long queueHeartbeatInterval = Constants.HEARTBEAT_INTERVAL_SECONDS_DEFAULT;
    
    private AmazonSQSResponderClientBuilder() {
    }
    
    public static AmazonSQSResponder defaultClient() {
        return standard().build();
    }
    
    public Optional<SqsClient> getAmazonSQS() {
        return customSQS;
    }
    
    public void setAmazonSQS(SqsClient sqs) {
        this.customSQS = Optional.of(sqs);
    }
    
    public AmazonSQSResponderClientBuilder withAmazonSQS(SqsClient sqs) {
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

    public long getQueueHeartbeatInterval() {
        return queueHeartbeatInterval;
    }

    public void setQueueHeartbeatInterval(long queueHeartbeatInterval) {
        this.queueHeartbeatInterval = queueHeartbeatInterval;
    }

    public AmazonSQSResponderClientBuilder withQueueHeartbeatInterval(long heartbeatIntervalSeconds) {
        setQueueHeartbeatInterval(heartbeatIntervalSeconds);
        return this;
    }

	/**
     * @return Create new instance of builder with all defaults set.
     */
    public static AmazonSQSResponderClientBuilder standard() {
        return new AmazonSQSResponderClientBuilder();
    }
    
    public AmazonSQSResponder build() {
        SqsClient sqs = customSQS.orElseGet(SqsClient::create);
        SqsClient deleter = new AmazonSQSIdleQueueDeletingClient(sqs, internalQueuePrefix, queueHeartbeatInterval);
        SqsClient virtualQueuesClient = AmazonSQSVirtualQueuesClientBuilder.standard().withAmazonSQS(deleter)
                .withHeartbeatIntervalSeconds(queueHeartbeatInterval).build();
        return new AmazonSQSResponderClient(virtualQueuesClient);
    }
}
