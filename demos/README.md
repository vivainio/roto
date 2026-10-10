# Demos

Small, runnable experiments that exercise Roto from external tools and SDKs.

- [CDK deployment against local Roto](cdk/README.md): synthesize a TypeScript
  CDK app and attempt to deploy its CloudFormation stack to Roto.
- [CloudFormation demo](cloudformation/README.md): create queues and a topic,
  use a nested stack, then import an exported value from a second stack.
- [Lambda container demo](lambda-container/README.md): build a small Lambda image
  and invoke it through RIE using a Lua executor binding.
