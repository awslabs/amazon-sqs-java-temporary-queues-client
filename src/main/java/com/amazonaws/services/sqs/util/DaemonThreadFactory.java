package com.amazonaws.services.sqs.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DaemonThreadFactory implements ThreadFactory {
    static AtomicInteger threadCount = new AtomicInteger(0);

    private final String factoryID;

    public DaemonThreadFactory(String factoryID) {
        this.factoryID = factoryID;
    }

    @Override
    public Thread newThread(Runnable r) {
        int threadNumber = threadCount.addAndGet(1);
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName(factoryID + "-Thread-" + threadNumber);
        return thread;
    }
}
