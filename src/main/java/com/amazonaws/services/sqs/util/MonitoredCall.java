package com.amazonaws.services.sqs.util;

import com.amazonaws.monitoring.ApiCallAttemptMonitoringEvent;
import com.amazonaws.monitoring.ApiCallMonitoringEvent;
import com.amazonaws.monitoring.MonitoringListener;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public enum MonitoredCall {
    CreateVirtualQueue;

    public <T> T monitor(MonitoringListener monitoringListener, Supplier<T> supplier) {
        long startNanos = System.nanoTime();
        long latencyNanos;
        Exception thrown = null;
        try {
            T result = supplier.get();
            latencyNanos = System.nanoTime() - startNanos;
            ApiCallMonitoringEvent event = new ApiCallMonitoringEvent()
                    .withApi(name())
                    .withLatency(TimeUnit.NANOSECONDS.toMillis(latencyNanos));
            monitoringListener.handleEvent(event);
            return result;
        } catch (Exception e) {
            thrown = e;
            throw e;
        } finally {
            latencyNanos = System.nanoTime() - startNanos;
            ApiCallAttemptMonitoringEvent event = new ApiCallAttemptMonitoringEvent()
                    .withApi(name())
                    .withAttemptLatency(TimeUnit.NANOSECONDS.toMillis(latencyNanos))
                    .withSdkException(thrown.getClass().getName());
            monitoringListener.handleEvent(event);
        }
    }
}
