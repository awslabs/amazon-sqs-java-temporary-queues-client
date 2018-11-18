package com.amazonaws.services.sqs;


public class VirtualQueueID {

    private final String hostQueueUrl;
    private final String virtualQueueName;
   
    public static VirtualQueueID fromQueueUrl(String queueUrl) {
        int index = queueUrl.indexOf('#');
        if (index >= 0) {
            return new VirtualQueueID(queueUrl.substring(0, index), queueUrl.substring(index + 1));
        } else {
            return null;
        }
    }
    
    public VirtualQueueID(String hostQueueUrl, String virtualQueueName) {
        this.hostQueueUrl = hostQueueUrl;
        this.virtualQueueName = virtualQueueName;
    }
    
    public String getHostQueueUrl() {
        return hostQueueUrl;
    }
    
    public String getVirtualQueueName() {
        return virtualQueueName;
    }
    
    public String getQueueUrl() {
        return hostQueueUrl + '#' + virtualQueueName;
    }
}
