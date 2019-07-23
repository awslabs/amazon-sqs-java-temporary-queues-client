package com.amazonaws.services.sqs.util;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;

import java.util.function.Consumer;

public class SQSMessageConsumerBuilder {

    private AmazonSQS amazonSQS = null;

    private String queueUrl = null;

    private Consumer<Message> consumer = null;

    private Consumer<Exception> exceptionHandler = SQSQueueUtils.DEFAULT_EXCEPTION_HANDLER;

    private Runnable shutdownHook = () -> {};

    private int maxWaitTimeSeconds = 20;

    private int pollingThreadCount = 1;

    private SQSMessageConsumerBuilder() {
    }

    public AmazonSQS getAmazonSQS() {
        return amazonSQS;
    }

    public void setAmazonSQS(AmazonSQS amazonSQS) {
        this.amazonSQS = amazonSQS;
    }

    public SQSMessageConsumerBuilder withAmazonSQS(AmazonSQS amazonSQS) {
        setAmazonSQS(amazonSQS);
        return this;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public void setQueueUrl(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    public SQSMessageConsumerBuilder withQueueUrl(String queueUrl) {
        setQueueUrl(queueUrl);
        return this;
    }

    public Consumer<Message> getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer<Message> consumer) {
        this.consumer = consumer;
    }

    public SQSMessageConsumerBuilder withConsumer(Consumer<Message> consumer) {
        setConsumer(consumer);
        return this;
    }

    public Consumer<Exception> getExceptionHandler() {
        return exceptionHandler;
    }

    public void setExceptionHandler(Consumer<Exception> exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    public SQSMessageConsumerBuilder withExceptionHandler(Consumer<Exception> exceptionHandler) {
        setExceptionHandler(exceptionHandler);
        return this;
    }

    public Runnable getShutdownHook() {
        return shutdownHook;
    }

    public void setShutdownHook(Runnable shutdownHook) {
        this.shutdownHook = shutdownHook;
    }

    public SQSMessageConsumerBuilder withShutdownHook(Runnable shutdownHook) {
        setShutdownHook(shutdownHook);
        return this;
    }

    public int getMaxWaitTimeSeconds() {
        return maxWaitTimeSeconds;
    }

    public void setMaxWaitTimeSeconds(int waitTimeSeconds) {
        this.maxWaitTimeSeconds = maxWaitTimeSeconds;
    }

    public SQSMessageConsumerBuilder withMaxWaitTimeSeconds(int maxWaitTimeSeconds) {
        setMaxWaitTimeSeconds(maxWaitTimeSeconds);
        return this;
    }

    public int getPollingThreadCount() {
        return pollingThreadCount;
    }

    public void setPollingThreadCount(int pollingThreadCount) {
        this.pollingThreadCount = pollingThreadCount;
    }

    public SQSMessageConsumerBuilder withPollingThreadCount(int pollingThreadCount) {
        setPollingThreadCount(pollingThreadCount);
        return this;
    }

    public static SQSMessageConsumerBuilder standard() {
        return new SQSMessageConsumerBuilder();
    }

    public SQSMessageConsumer build() {
        return new SQSMessageConsumer(amazonSQS, queueUrl, consumer, shutdownHook, exceptionHandler,
                                      maxWaitTimeSeconds, pollingThreadCount);
    }
}
