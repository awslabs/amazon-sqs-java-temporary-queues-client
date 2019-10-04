package com.amazonaws.services.sqs.util;

import com.amazonaws.monitoring.ApiCallAttemptMonitoringEvent;
import com.amazonaws.monitoring.ApiCallMonitoringEvent;
import com.amazonaws.monitoring.MonitoringEvent;
import com.amazonaws.monitoring.MonitoringListener;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public enum MonitoredCall {
    CreateVirtualQueue;
}
