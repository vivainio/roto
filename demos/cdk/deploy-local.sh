#!/bin/sh
set -eu

endpoint="${ROTO_ENDPOINT:-http://localhost:5071}"

# AWS SDK endpoint environment variables route the CDK CLI's AWS API requests
# to Roto. Use an unmistakable fake credential; never put real AWS credentials
# in this demo environment.
export AWS_ENDPOINT_URL="$endpoint"
export AWS_ACCESS_KEY_ID=ROTOdemo
export AWS_SECRET_ACCESS_KEY=ROTOdemo
export AWS_DEFAULT_REGION=us-east-1
export CDK_DEFAULT_ACCOUNT=123456789012
export CDK_DEFAULT_REGION=us-east-1

# Deploy every stack in the app unless specific stack names are given.
if [ "$#" -eq 0 ]; then
  set -- --all
fi

exec npm run deploy -- --method direct "$@"
