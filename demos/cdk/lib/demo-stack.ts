import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

export class RotoDemoStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, {
      ...props,
      // This demo has no file or Docker assets and should not require a real
      // CDK bootstrap bucket or deployment roles.
      synthesizer: new cdk.BootstraplessSynthesizer(),
    });

    const queue = new sqs.Queue(this, 'Jobs');
    new lambda.Function(this, 'Worker', {
      runtime: lambda.Runtime.NODEJS_24_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(
        'exports.handler = async () => ({ statusCode: 200 });',
      ),
      environment: { QUEUE_URL: queue.queueUrl },
    });
  }
}
