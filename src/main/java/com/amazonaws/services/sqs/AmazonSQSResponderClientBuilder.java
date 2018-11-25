package com.amazonaws.services.sqs;

import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.client.AwsSyncClientParams;
import com.amazonaws.client.builder.AwsSyncClientBuilder;

public class AmazonSQSResponderClientBuilder extends AwsSyncClientBuilder<AmazonSQSResponderClientBuilder, AmazonSQSResponder> {

    private static final ClientConfigurationFactory CLIENT_CONFIG_FACTORY = new com.amazonaws.services.sqs.AmazonSQSClientConfigurationFactory();

    private AmazonSQSResponderClientBuilder() {
        super(CLIENT_CONFIG_FACTORY);
    }

    public static AmazonSQSResponder build(AmazonSQS sqs) {
        return new AmazonSQSResponsesClient(AmazonSQSTemporaryQueuesClient.make(sqs));
    }

    @Override
    protected AmazonSQSResponder build(AwsSyncClientParams clientParams) {
        return build(AmazonSQSClientBuilder.standard().build(clientParams));
    }
}
