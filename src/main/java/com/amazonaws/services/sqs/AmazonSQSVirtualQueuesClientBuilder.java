package com.amazonaws.services.sqs;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.util.Constants;

import java.util.Optional;
import java.util.function.BiConsumer;

public class AmazonSQSVirtualQueuesClientBuilder {

    private AmazonSQS amazonSQS;

    private Optional<BiConsumer<String, Message>> messageHandler = Optional.empty();

    private BiConsumer<String, Message> orphanedMessageHandler = AmazonSQSVirtualQueuesClient.DEFAULT_ORPHANED_MESSAGE_HANDLER;

    private int hostQueuePollingThreads = 1;

    private int maxWaitTimeSeconds = 20;

    private long heartbeatIntervalSeconds = Constants.HEARTBEAT_INTERVAL_SECONDS_DEFAULT;

    private AmazonSQSVirtualQueuesClientBuilder() {
    }

    public AmazonSQS getAmazonSQS() {
        return amazonSQS;
    }

    public void setAmazonSQS(AmazonSQS amazonSQS) {
        this.amazonSQS = amazonSQS;
    }

    public AmazonSQSVirtualQueuesClientBuilder withAmazonSQS(AmazonSQS amazonSQS) {
        setAmazonSQS(amazonSQS);
        return this;
    }

    /**
     * @see #setMessageHandler
     */
    public Optional<BiConsumer<String, Message>> getMessageHandler() {
        return messageHandler;
    }

    /**
     * Sets a message consumer to handle all messages sent to virtual queues created by this client.
     * If this is not empty, it will be invoked on messages as they are received from the host queue
     * <b>INSTEAD</b> of making them available to ReceiveMessage calls on virtual queue URLs. This means if
     * this is not empty, all receives on virtual queues will always be empty.
     *
     * <p>
     *
     * Note that this callback will be invoked by internal threads managed by this client. It is <b>strongly recommended</b>
     * that this callback does not do significant processing or blocking operations, as this may delay the delivery
     * of other virtual queue messages!
     * @return
     */
    public void setMessageHandler(Optional<BiConsumer<String, Message>> messageHandler) {
        this.messageHandler = messageHandler;
    }

    /**
     * @see #setMessageHandler
     */
    public AmazonSQSVirtualQueuesClientBuilder withMessageHandler(Optional<BiConsumer<String, Message>> messageHandler) {
        setMessageHandler(messageHandler);
        return this;
    }

    /**
     * @see #setOrphanedMessageHandler
     */
    public BiConsumer<String, Message> getOrphanedMessageHandler() {
        return orphanedMessageHandler;
    }

    /**
     * Sets a message consumer to handle all messages sent to virtual queues that do not exist on the host queue.
     * The default behaviour is to emit a log and leave the message inflight.
     *
     * <p>
     *
     * Note that this callback will be invoked by internal threads managed by this client. It is <b>strongly recommended</b>
     * that this callback does not do significant processing or blocking operations, as this may delay the delivery
     * of other virtual queue messages!
     * @return
     */
    public void setOrphanedMessageHandler(BiConsumer<String, Message> orphanedMessageHandler) {
        this.orphanedMessageHandler = orphanedMessageHandler;
    }

    /**
     * @see #setOrphanedMessageHandler
     */
    public AmazonSQSVirtualQueuesClientBuilder withOrphanedMessageHandler(BiConsumer<String, Message> orphanedMessageHandler) {
        setOrphanedMessageHandler(orphanedMessageHandler);
        return this;
    }

    public int getHostQueuePollingThreads() {
        return hostQueuePollingThreads;
    }

    public void setHostQueuePollingThreads(int hostQueuePollingThreads) {
        this.hostQueuePollingThreads = hostQueuePollingThreads;
    }

    public AmazonSQSVirtualQueuesClientBuilder withHostQueuePollingThreads(int hostQueuePollingThreads) {
        setHostQueuePollingThreads(hostQueuePollingThreads);
        return this;
    }

    public int getMaxWaitTimeSeconds() {
        return maxWaitTimeSeconds;
    }

    public void setMaxWaitTimeSeconds(int maxWaitTimeSeconds) {
        this.maxWaitTimeSeconds = maxWaitTimeSeconds;
    }

    public AmazonSQSVirtualQueuesClientBuilder withMaxWaitTimeSeconds(int maxWaitTimeSeconds) {
        setMaxWaitTimeSeconds(maxWaitTimeSeconds);
        return this;
    }

    public long getHeartbeatIntervalSeconds() {
        return heartbeatIntervalSeconds;
    }

    public void setHeartbeatIntervalSeconds(long heartbeatIntervalSeconds) {
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    }

    public AmazonSQSVirtualQueuesClientBuilder withHeartbeatIntervalSeconds(long heartbeatIntervalSeconds) {
        setHeartbeatIntervalSeconds(heartbeatIntervalSeconds);
        return this;
    }

    /**
     * @return Create new instance of builder with all defaults set.
     */
    public static AmazonSQSVirtualQueuesClientBuilder standard() {
        return new AmazonSQSVirtualQueuesClientBuilder();
    }

    public AmazonSQS build() {
        return new AmazonSQSVirtualQueuesClient(amazonSQS, messageHandler, orphanedMessageHandler,
                                                hostQueuePollingThreads, maxWaitTimeSeconds, heartbeatIntervalSeconds);
    }
}
