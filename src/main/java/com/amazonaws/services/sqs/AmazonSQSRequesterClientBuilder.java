package com.amazonaws.services.sqs;

import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.client.AwsSyncClientParams;
import com.amazonaws.client.builder.AwsSyncClientBuilder;

public class AmazonSQSRequesterClientBuilder extends AwsSyncClientBuilder<AmazonSQSRequesterClientBuilder, AmazonSQSRequester> {
    
    private static final ClientConfigurationFactory CLIENT_CONFIG_FACTORY = new com.amazonaws.services.sqs.AmazonSQSClientConfigurationFactory();

    private boolean useTemporaryQueues;
    
    private AmazonSQSRequesterClientBuilder() {
        super(CLIENT_CONFIG_FACTORY);
    }

    public static AmazonSQSRequester build(AmazonSQS sqs) {
        return new AmazonSQSResponsesClient(AmazonSQSTemporaryQueuesClient.make(sqs));
    }

    @Override
    protected AmazonSQSRequester build(AwsSyncClientParams clientParams) {
        return build(AmazonSQSClientBuilder.standard().build(clientParams));
    }
}
