#!/bin/sh
set -eu

endpoint="${ROTO_ENDPOINT:-http://localhost:5071}"
endpoint="${endpoint%/}"
bucket="roto-cloudformation-demo-assets"
suffix="$(date +%s)-$$"
stack_name="RotoCloudFormationDemo-${suffix}"
consumer_stack_name="${stack_name}-Consumer"
environment="demo-${suffix}"
export_name="${stack_name}-NotificationsQueueArn"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

if ! command -v aws >/dev/null 2>&1; then
  echo "Install the AWS CLI to run this demo." >&2
  exit 1
fi

# Route all AWS CLI calls to local Roto with fake credentials.
export AWS_ACCESS_KEY_ID=ROTOdemo
export AWS_SECRET_ACCESS_KEY=ROTOdemo
export AWS_DEFAULT_REGION=us-east-1

aws_local() {
  aws --endpoint-url "$endpoint" "$@"
}

if ! aws_local s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
  aws_local s3api create-bucket --bucket "$bucket" >/dev/null
fi
aws_local s3api put-object \
  --bucket "$bucket" \
  --key nested-stack.yaml \
  --body "$script_dir/nested-stack.yaml" >/dev/null

sed "s|@@TEMPLATE_URL@@|${endpoint}/${bucket}/nested-stack.yaml|g" \
  "$script_dir/stack.yaml" > "$tmpdir/stack.yaml"

stack_id=$(aws_local cloudformation create-stack \
  --stack-name "$stack_name" \
  --template-body "file://$tmpdir/stack.yaml" \
  --parameters "ParameterKey=Environment,ParameterValue=$environment" \
  --tags Key=demo,Value=cloudformation \
  --query StackId \
  --output text)

echo "Created main stack: $stack_name ($stack_id)"
aws_local cloudformation describe-stacks \
  --stack-name "$stack_id" \
  --output table

consumer_id=$(aws_local cloudformation create-stack \
  --stack-name "$consumer_stack_name" \
  --template-body "file://$script_dir/consumer.yaml" \
  --parameters "ParameterKey=QueueArnExportName,ParameterValue=$export_name" \
  --tags Key=demo,Value=cloudformation-consumer \
  --query StackId \
  --output text)

echo "Created importing stack: $consumer_stack_name ($consumer_id)"
aws_local cloudformation describe-stacks \
  --stack-name "$consumer_id" \
  --output table

child_stack_id=$(aws_local cloudformation describe-stacks \
  --stack-name "$stack_id" \
  --query 'Stacks[0].Outputs[?OutputKey==`ChildStackId`].OutputValue | [0]' \
  --output text)
echo "Nested stack metadata:"
aws_local cloudformation list-stacks \
  --query "StackSummaries[?StackId=='$child_stack_id'].[StackName,StackStatus,ParentId,RootId]" \
  --output table

echo "Export consumed by the second stack:"
aws_local cloudformation list-exports \
  --query "Exports[?Name=='$export_name'].[Name,Value]" \
  --output table

echo "Delete when finished, in this order:"
echo "aws --endpoint-url '$endpoint' cloudformation delete-stack --stack-name '$consumer_stack_name'"
echo "aws --endpoint-url '$endpoint' cloudformation delete-stack --stack-name '$stack_name'"
