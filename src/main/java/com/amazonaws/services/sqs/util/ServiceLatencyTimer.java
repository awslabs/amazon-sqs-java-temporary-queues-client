package com.amazonaws.services.sqs.util;

import com.amazonaws.metrics.AwsSdkMetrics;
import com.amazonaws.metrics.ServiceLatencyProvider;
import com.amazonaws.metrics.ServiceMetricType;
import com.amazonaws.metrics.SimpleServiceMetricType

import java.util.function.Supplier;

public class ServiceLatencyTimer {

    public static <T> Supplier<T> withTiming(String serviceName, String operationName, Supplier<T> supplier) {
        return () -> {
            ServiceMetricType metricType = new SimpleServiceMetricType(operationName, serviceName);
            ServiceLatencyProvider latencyProvider = new ServiceLatencyProvider(metricType);
            String exceptionName = null;
            try {
                return supplier.get();
            } catch (RuntimeException e) {
                exceptionName = e.getClass().getSimpleName();
                zeroLatencyMetric(serviceName, operationName + ":Exception:" + exceptionName);
                throw e;
            } finally {
                AwsSdkMetrics.getServiceMetricCollector()
                        .collectLatency(latencyProvider.endTiming());
            }
        };
    }

    public static Runnable withTiming(String serviceName, String operationName, Runnable runnable) {
        return () -> {
            ServiceMetricType metricType = new SimpleServiceMetricType(operationName, serviceName);
            ServiceLatencyProvider latencyProvider = new ServiceLatencyProvider(metricType);
            String exceptionName = null;
            try {
                runnable.run();
            } catch (RuntimeException e) {
                exceptionName = e.getClass().getSimpleName();
                zeroLatencyMetric(serviceName, operationName + ":Exception:" + exceptionName);
                throw e;
            } finally {
                AwsSdkMetrics.getServiceMetricCollector()
                        .collectLatency(latencyProvider.endTiming());
            }
        };
    }

    public static void zeroLatencyMetric(String serviceName, String metricName) {
        ServiceMetricType metricType = new SimpleServiceMetricType(metricName, serviceName);
        AwsSdkMetrics.getServiceMetricCollector()
                .collectLatency(new ServiceLatencyProvider(metricType).endTiming());
    }
}
