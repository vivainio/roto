import * as cdk from 'aws-cdk-lib';
import { RotoDataStack } from '../lib/data-stack';
import { RotoDemoStack } from '../lib/demo-stack';
import { RotoMessagingStack } from '../lib/messaging-stack';

const app = new cdk.App();
const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT ?? '123456789012',
  region: process.env.CDK_DEFAULT_REGION ?? 'us-east-1',
};
new RotoDemoStack(app, 'RotoCdkDemo', { env });
new RotoDataStack(app, 'RotoCdkData', { env });
new RotoMessagingStack(app, 'RotoCdkMessaging', { env });
