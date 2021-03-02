package com.amazonaws.services.sqs;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class AmazonSQSTemporaryQueuesClientBuilder {

    public final static long IDLE_QUEUE_RETENTION_PERIOD_SECONDS_DEFAULT = 300;

    private AmazonSQSRequesterClientBuilder requesterBuilder = AmazonSQSRequesterClientBuilder.standard();
    
    private AmazonSQSTemporaryQueuesClientBuilder() {
    }
    
    public Optional<AmazonSQS> getAmazonSQS() {
        return requesterBuilder.getAmazonSQS();
    }
    
    public void setAmazonSQS(AmazonSQS sqs) {
        requesterBuilder.setAmazonSQS(sqs);
    }
    
    public AmazonSQSTemporaryQueuesClientBuilder withAmazonSQS(AmazonSQS sqs) {
        setAmazonSQS(sqs);
        return this;
    }

    public String getQueuePrefix() {
        return requesterBuilder.getInternalQueuePrefix();
    }
    
    public void setQueuePrefix(String queuePrefix) {
        requesterBuilder.setInternalQueuePrefix(queuePrefix);
    }
    
    public AmazonSQSTemporaryQueuesClientBuilder withQueuePrefix(String queuePrefix) {
        setQueuePrefix(queuePrefix);
        return this;
    }

    public long getIdleQueueRetentionPeriodSeconds() {
        return requesterBuilder.getIdleQueueRetentionPeriodSeconds();
    }

    public void setIdleQueueRetentionPeriodSeconds(long queueRetentionPeriodSeconds) {
        requesterBuilder.setIdleQueueRetentionPeriodSeconds(queueRetentionPeriodSeconds);
    }

    public AmazonSQSTemporaryQueuesClientBuilder withIdleQueueRetentionPeriodSeconds(long idleQueueRetentionPeriodSeconds) {
        setIdleQueueRetentionPeriodSeconds(idleQueueRetentionPeriodSeconds);
        return this;
    }

    public int getIdleQueueSweepingPeriod() {
        return requesterBuilder.getIdleQueueSweepingPeriod();
    }
    
    public TimeUnit getIdleQueueSweepingTimeUnit() {
        return requesterBuilder.getIdleQueueSweepingTimeUnit();
    }
    
    public void setIdleQueueSweepingPeriod(int period, TimeUnit timeUnit) {
        requesterBuilder.setIdleQueueSweepingPeriod(period, timeUnit);
    }
    
    public AmazonSQSTemporaryQueuesClientBuilder withIdleQueueSweepingPeriod(int period, TimeUnit timeUnit) {
        setIdleQueueSweepingPeriod(period, timeUnit);
        return this;
    }

    public Map<String, String> getQueueTags() {
        return requesterBuilder.getQueueTags();
    }

    public void setQueueTags(Map<String, String> queueTags) {
        requesterBuilder.setQueueTags(queueTags);
    }

    public AmazonSQSTemporaryQueuesClientBuilder withQueueTags(Map<String, String> queueTags) {
        setQueueTags(queueTags);
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
        return AmazonSQSTemporaryQueuesClient.make(requesterBuilder).getWrappedClient();
    }
}
