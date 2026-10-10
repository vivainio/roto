# Try an AWS CDK deployment against Roto

This demo asks the CDK CLI to deploy three stacks to a local Roto server. It
is an experiment in how far a real CDK deployment gets; a failed deployment is
useful output. The stacks use only plain CloudFormation resources, so CDK
synthesizes normal templates without publishing Lambda assets or bootstrapping
an AWS account:

- `RotoCdkDemo`: an SQS queue, an IAM role, and an inline Lambda function.
- `RotoCdkData`: an S3 bucket, a DynamoDB table, and a Kinesis stream. The
  stream's encryption setting is synthesized as a `Conditions` section.
- `RotoCdkMessaging`: an SNS topic and two SQS queues.

From the repository root, start Roto with the reusable demo fixture:

```sh
scripts/demo.sh
```

In another terminal:

```sh
cd demos/cdk
npm install
./deploy-local.sh
```

The script sets the AWS SDK endpoint to `http://localhost:5071` (or
`ROTO_ENDPOINT` if supplied) and uses fake `ROTOdemo` credentials. It does not
run `cdk bootstrap`. Use no real AWS credentials with this experiment. To try a
different local Roto port:

```sh
ROTO_ENDPOINT=http://localhost:5072 ./deploy-local.sh
```

`deploy-local.sh` deploys all three stacks by default; pass stack names to
deploy a subset. CDK still performs its normal synth/deploy workflow. Roto may reject API
operations or CloudFormation resource types that it does not implement; the
terminal output and Roto trace are the things to inspect. The demo intentionally
does not claim that the stack becomes usable. The `BootstraplessSynthesizer`
avoids CDK bootstrap assets and roles, while the inline Lambda keeps asset
publishing out of the experiment.

All three stacks reach `CREATE_COMPLETE`. A second run reports
`(no changes)`, and adding a resource to a stack goes through `UpdateStack` to
`UPDATE_COMPLETE`. Stack events are recorded for every status change, so CDK's
event poll completes. Roto supports create/update change sets, stack policies,
termination protection, rollback on failure, and standard/forced deletion.
Rollback triggers, nested change sets, and resource imports remain unsupported.

CDK deployments normally synthesize a CloudFormation template and submit it to
CloudFormation, with assets uploaded separately when present. This demo keeps
assets out so the experiment focuses on the CloudFormation API path. See the
[CDK deployment guide](https://docs.aws.amazon.com/cdk/v2/guide/deploy.html)
for the normal AWS deployment flow.
