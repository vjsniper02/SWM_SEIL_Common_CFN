# Azure DevOps Integration

The pipelines make use of the [AWS Toolkit for Azure DevOps](https://aws.amazon.com/vsts/).

There are three service connections to the corresponding AWS Accounts:

| Service connection | Account      | Role                      |
|--------------------|--------------|---------------------------|
| AwsDev             | [REDACTED_AWS_ACCOUNT_ID] | CloudFormationAzureDevops |
| AwsNonProd         | 019092404871 | CloudFormationAzureDevops |
| AwsProd            | 363801000918 | CloudFormationAzureDevops |

The user and role are created by cloud formation `cfn/templates/platform/account-azureDevOps.yaml` and the credentials are stored in AWS Secrets Manager as **CloudFormationAzdo**.
