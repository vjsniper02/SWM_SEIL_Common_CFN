# CloudFormation

There are two pipelines, one for account level cfn and one for environment specific

## Account create or update stack

    pipelines/account-create-update-stack.yaml

This is used to deploy stacks that are account wide, e.g. the credentials used for the Azure Devops Service Connection

## Environment create or update stack

    pipelines/environment-create-update-stack.yaml

This is used to deploy stacks that are environment specific.  Based on the environment selected, it will determine the AWS account to deploy to.

## Troubleshooting stack deployment

###Pipeline fails to complete with "failed to reach update completion status" error

```
Waiting for stack platform-dev-ecr to reach update complete status
Stack update request failed with error:  
Error: Stack platform-dev-ecr failed to reach update completion status,
error: 'Resource is not in the state stackUpdateComplete'
```

### Resolution
Update the stack from CLI:

```sh
aws cloudformation update-stack \
--capabilities CAPABILITY_AUTO_EXPAND CAPABILITY_IAM CAPABILITY_NAMED_IAM \
--stack-name platform-dev-ecr \
--template-body "$(cat cfn/templates/platform/environment-ecr.yaml)" \
--parameter ParameterKey=EnvPrefix,ParameterValue=dev
```