## Amazon SQS Java Temporary Queue Client

The Temporary Queue Client lets you create lightweight, temporary queues that are deleted automatically when they are no longer in use. You can use the Temporary Queue Client for use in common messaging patterns such as Request-Response.

This library provides two complementary interfaces for two-way communication through queues:

* The [`AmazonSQSRequester`](./src/main/java/com/amazonaws/services/sqs/AmazonSQSRequester.java) interface lets message producers send a message and wait for the corresponding response message.
* The [`AmazonSQSResponder`](./src/main/java/com/amazonaws/services/sqs/AmazonSQSResponder.java) interface lets message consumers send response messages.

To implement this pattern efficiently, the `AmazonSQSRequester` client creates temporary queues that hold response messages. The temporary queue architecture scales to an arbitrary number of message producer runtimes. There is no risk of response messages being consumed by the wrong client.

Temporary queues are also automatically deleted if the clients that created them die ungracefully. By default, these internal queues are created with the queue name prefix `__RequesterClientQueues__`. You can configure this prefix when you [build the requester client](./src/main/java/com/amazonaws/services/sqs/AmazonSQSRequesterClientBuilder.java).

## Getting Started

1. Before you begin, register for an AWS account. For more information about creating an AWS account and retrieving your AWS credentials, see [AWS Account and Credentials](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html) in the _AWS SDK for Java Developer Guide._
2. Sign in to the [Amazon SQS console](https://console.aws.amazon.com/sqs/home?region=us-east-1).
3. To use the Temporary Queue client, you'll need [Java 8 (or later)](https://www.java.com/en/download/) and [Maven 3](http://maven.apache.org/).
4. [Download the latest release](https://github.com/awslabs/amazon-sqs-java-temporary-queues-client/releases) or add a Maven dependency into your `pom.xml` file:
### Version 2.x
```xml
  <dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>amazon-sqs-java-temporary-queues-client</artifactId>
    <version>2.0.0</version>
    <type>jar</type>
  </dependency>
```

### Version 1.x
```xml
  <dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>amazon-sqs-java-temporary-queues-client</artifactId>
    <version>1.2.4</version>
    <type>jar</type>
  </dependency>
```
5. [Explore the code examples](https://github.com/aws-samples/amazon-sqs-java-temporary-queues-client-samples).
6. [Read the documentation](http://aws.amazon.com/documentation/sqs/).

## Feedback
* [Send us your feedback](https://github.com/awslabs/amazon-sqs-java-temporary-queues-client/issues).
* If you'd like to contribute a new feature or bug fix, we'd love to see Github pull requests from you!

## License

This library is licensed under the Apache 2.0 License. 

See [LICENSE](./LICENSE) and [NOTICE](./NOTICE) for more information.
