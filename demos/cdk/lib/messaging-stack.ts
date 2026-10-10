import * as cdk from 'aws-cdk-lib';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

// Messaging resources: an SNS topic and an SQS queue for its consumers.
export class RotoMessagingStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, {
      ...props,
      synthesizer: new cdk.BootstraplessSynthesizer(),
    });

    new sns.Topic(this, 'Notifications');
    new sqs.Queue(this, 'Inbox');
    new sqs.Queue(this, 'Audit', { visibilityTimeout: cdk.Duration.seconds(60) });
  }
}
