package com.amazonaws.services.sqs;

import java.util.UUID;

// TODO-RS: Move back to test folder once I sort out the gradle build
public class TestUtils {

    static String getOdinMaterialSet() {
        return "com.amazonaws.sqs.test-prod-throttled.aws";
    }
    
    public static String generateRandomQueueName() {
        return "sqs-virt-queues-client-integ-test-queue" + UUID.randomUUID();
    }
}
