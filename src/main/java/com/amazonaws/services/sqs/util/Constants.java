package com.amazonaws.services.sqs.util;

public class Constants {
    public static final String RESPONSE_QUEUE_URL_ATTRIBUTE_NAME = "ResponseQueueUrl";
    public static final String VIRTUAL_QUEUE_HOST_QUEUE_ATTRIBUTE = "HostQueueUrl";
    public static final String IDLE_QUEUE_RETENTION_PERIOD = "IdleQueueRetentionPeriodSeconds";
    public static final long MINIMUM_IDLE_QUEUE_RETENTION_PERIOD_SECONDS = 1;
    public static final long HEARTBEAT_INTERVAL_SECONDS_DEFAULT = 5;
    public static final long HEARTBEAT_INTERVAL_SECONDS_MIN_VALUE = 1;
}
