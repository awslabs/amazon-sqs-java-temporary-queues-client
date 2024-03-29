package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.util.Constants;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class AmazonSQSRequesterClientBuilder {

    private Optional<SqsClient> customSQS = Optional.empty();

    private String internalQueuePrefix = "__RequesterClientQueues__";

    private Map<String, String> queueAttributes = Collections.emptyMap();

    private int idleQueueSweepingPeriod = 5;
    private long queueHeartbeatInterval = Constants.HEARTBEAT_INTERVAL_SECONDS_DEFAULT;
    private TimeUnit idleQueueSweepingTimeUnit = TimeUnit.MINUTES;
    private long idleQueueRetentionPeriodSeconds = AmazonSQSTemporaryQueuesClientBuilder.IDLE_QUEUE_RETENTION_PERIOD_SECONDS_DEFAULT;
    
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

    public Optional<SqsClient> getAmazonSQS() {
        return customSQS;
    }

    public void setAmazonSQS(SqsClient sqs) {
        this.customSQS = Optional.of(sqs);
    }

    public AmazonSQSRequesterClientBuilder withAmazonSQS(SqsClient sqs) {
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

    public int getIdleQueueSweepingPeriod() {
        return idleQueueSweepingPeriod;
    }
    
    public TimeUnit getIdleQueueSweepingTimeUnit() {
        return idleQueueSweepingTimeUnit;
    }

    public void setIdleQueueSweepingPeriod(int period, TimeUnit timeUnit) {
        this.idleQueueSweepingPeriod = period;
        this.idleQueueSweepingTimeUnit = timeUnit;
    }

    public AmazonSQSRequesterClientBuilder withIdleQueueSweepingPeriod(int period, TimeUnit timeUnit) {
        setIdleQueueSweepingPeriod(period, timeUnit);
        return this;
    }

    public long getIdleQueueRetentionPeriodSeconds() {
        return idleQueueRetentionPeriodSeconds;
    }

    public void setIdleQueueRetentionPeriodSeconds(long idleQueueRetentionPeriodSeconds) {
        this.idleQueueRetentionPeriodSeconds = idleQueueRetentionPeriodSeconds;
    }

    public AmazonSQSRequesterClientBuilder withIdleQueueRetentionPeriodSeconds(long idleQueueRetentionPeriodSeconds) {
        setIdleQueueRetentionPeriodSeconds(idleQueueRetentionPeriodSeconds);
        return this;
    }

    public long getQueueHeartbeatInterval() {
        return queueHeartbeatInterval;
    }

    public void setQueueHeartbeatInterval(long heartbeatIntervalSeconds) {
        this.queueHeartbeatInterval = heartbeatIntervalSeconds;
    }

    public AmazonSQSRequesterClientBuilder withQueueHeartbeatInterval(long heartbeatIntervalSeconds) {
        setQueueHeartbeatInterval(heartbeatIntervalSeconds);
        return this;
    }

    public AmazonSQSRequester build() {
        return AmazonSQSTemporaryQueuesClient.make(this).getRequester();
    }

}
