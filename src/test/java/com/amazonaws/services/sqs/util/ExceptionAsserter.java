package com.amazonaws.services.sqs.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExceptionAsserter implements Consumer<Exception>, AutoCloseable {

    private static final Log LOG = LogFactory.getLog(ExceptionAsserter.class);

    private final List<Exception> exceptions = new ArrayList<>();
    
    @Override
    public void accept(Exception t) {
        exceptions.add(t);
    }
    
    public void assertNothingThrown() {
        if (!exceptions.isEmpty()) {
            Exception first = exceptions.get(0);
            AssertionError failure = new AssertionError("Unexpected exception", first);
            exceptions.subList(1, exceptions.size()).forEach(failure::addSuppressed);
            exceptions.clear();
            throw failure;
        }
    }
    
    @Override
    public void close() throws Exception {
        assertNothingThrown();
    }
}
