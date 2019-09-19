package com.amazonaws.services.sqs.util;

import com.amazonaws.metrics.AwsSdkMetrics;
import com.amazonaws.metrics.ServiceLatencyProvider;
import com.amazonaws.metrics.ServiceMetricType;

import java.util.Arrays;
import java.util.function.Supplier;

public enum ServiceLatencyMetric implements ServiceMetricType {
    IdleQueueSweep, CheckQueueForIdleness, DeleteIdleQueue,
    CreateVirtualQueue, DeleteVirtualQueue,
    Exception;

    static {
        AwsSdkMetrics.addAll(Arrays.asList(ServiceLatencyMetric.values()));
    }

    @Override
    public String getServiceName() {
        return "AmazonSQS";
    }

    public <T> T measureLatency(Supplier<T> supplier) {
        ServiceLatencyProvider latencyProvider = new ServiceLatencyProvider(this);
        try {
            return supplier.get();
        } catch (RuntimeException e) {
            Exception.addCount();
            throw e;
        } finally {
            AwsSdkMetrics.getServiceMetricCollector()
                    .collectLatency(latencyProvider.endTiming());
        }
    }

    public void measureLatency(Runnable runnable) {
        ServiceLatencyProvider latencyProvider = new ServiceLatencyProvider(this);
        try {
            runnable.run();
        } catch (RuntimeException e) {
            Exception.addCount();
            throw e;
        } finally {
            AwsSdkMetrics.getServiceMetricCollector()
                    .collectLatency(latencyProvider.endTiming());
        }
    }

    public void addCount() {
        // This isn't directly possible using the AwsSdkMetrics API, so we just add an
        // arbitrary latency so we can use the Sample Count.
        measureLatency(() -> {});
    }
}
