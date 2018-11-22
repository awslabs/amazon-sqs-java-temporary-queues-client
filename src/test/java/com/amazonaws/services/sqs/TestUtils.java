package com.amazonaws.services.sqs;

import java.util.UUID;

public class TestUtils {

    protected String generateRandomQueueName() {
        return generateRandomQueueName(getClass().getSimpleName());
    }
    
    protected String generateRandomQueueName(String basename) {
        return basename + "-" + UUID.randomUUID();
    }
}
