# CloudFormation demo

This demo uses CloudFormation to create an SQS queue and an SNS topic subscribed
to that queue, then adds a nested stack that creates another queue. It shows
parameters, `Ref`, `Fn::GetAtt`, `Fn::Sub`, outputs, stack exports, and
`Fn::ImportValue` between two stacks.

Start a fresh local Roto server from the repository root:

```sh
scripts/demo.sh
```

In another terminal, run the demo (requires AWS CLI):

```sh
cd demos/cloudformation
./deploy-local.sh
```

Set `ROTO_ENDPOINT` to use another local port. The script uses fake credentials
and unique stack and resource names for each run. It prints both stacks' outputs,
the nested stack metadata, and the export consumed by the second stack. Resources
can also be inspected at `http://localhost:5071/roto-api/`.

Each run leaves its stacks and the shared `roto-cloudformation-demo-assets`
bucket in the ephemeral server. Delete the consumer stack first, then the main
stack, using the commands printed by the script. Restarting `scripts/demo.sh`
clears all demo state.
