# Naming conventions

## Patterns for naming resources

The wider project "DevOps" documentation proposes `camelCase` for variable/object names.

As we have multiple environments existing in a single account, we often additionally need to prefix resources with the 
environment name (e.g. dev, etldev, sit, uat, etc.).

Although at first glance it might look weird, mixing camel with a hyphenated prefix will make things more usable 
(i.e. a hyphen between the environment prefix and the logical name).  This will make programmatic splitting or 
filtering via the AWS Console or CLI much easier.

Additionally, many AWS resources resolve back to a valid DNS name 
(e.g. S3) so casing is lost.  For these resources, naming components should be separated by a hyphen for readability.

Examples:

    dev-someLogicalName             # environment prefixed resource
    swmi-dev-someLogicalName        # globally unique project and environment prefixed resoure
    swmi-dev-appflow-landing        # DNS aware naming for a bucket

### Account wide resources

For example, networking (VPCs, route tables, IGWs etc).

Resources that are account wide don't need an environment prefix so just the logical name in `camelCase`.

Example:

```yaml
Resources:
  Foo:
    Type: AWS::Foo::Bar
    Properties:
      Name: fooBar
```

### Environment specific resources

Multiple environments exist in each account, as such in order to namespace them should be prefixed to the start of the resource.

The prefix should be passed to the CFN template as a parameter named `EnvPrefix`

Example:

```yaml
Parameters:
  EnvPrefix:
    Type: String
    Description: Environment Name as Prefix
    
Resources:
  Foo:
    Type: AWS::Foo::Bar
    Properties:
      Name: !Sub ${EnvPrefix}-fooBar
```

### Globally unique names

Some AWS services (e.g. S3, DynamoDB) require globally unique names.  For this there is a "ProjectPrefix" which is stored in SSM.

Example:

```yaml
Parameters:
  EnvPrefix:
    Type: String
    Description: Environment Name as Prefix
    
Resources:
  Foo:
    Type: AWS::Foo::Bar
    Properties:
      Name: !Sub {{resolve:ssm:ProjPrefix}}-${EnvPrefix}-fooBar
```

## Tagging of resouces

Resource tags should at a minimum have the below tags.

- CostCenter = Which team/department gets charged for it
- Environment = Phase
- IACReleaseNumber = Infrastructure release number (blank if not automated)
- LastUpdatedBy = The person who deployed the last update.
- Project = The system/Application the deployment is related to
- Role = The role within the deployment (blank if not automated)
- Repository = The repository that the item was deployed from (blank if not automated)
- Criticality = how detrimental is the resource to the business operation please see. Application Business Criticality - Classification
- PII = Does the resource contain Personally identifiable information
- SecurityClassification = Security classification for the system

As an example:

    tags: |
      CostCenter=${{ parameters.costCenter }}
      Environment=${{ parameters.env }}
      IACReleaseNumber=$(Build.SourceVersion)
      LastUpdatedBy=$(Build.QueuedBy)
      Project=${{ parameters.project }}
      Role=${{ parameters.role }}
      Repository=${{ parameters.repository }}
      Criticality=${{ parameters.criticality }}
      PII=${{ parameters.pii }}
      SecurityClassification=${{ parameters.securityClassification }}

## Cloud formation template naming

Templates live under `/cfn/templates` in the repo.  Templates that are platform specific under a `platform` and 
integration specific templates under their logical name.

For platform templates, there are two types, account level and enviroment level.  They should be prefixed as such 
because the pipeline treats them differently.  For environment specific ones, the word environment is substituted with
the environment being built in the stack name (e.g. environment-kms would become dev-kms for dev).

As an example, some files under `cfn/templates/platform`:

    cfn/templates/platform
    ├── README.md
    ├── account-appflowmulti-macro.yaml
    ├── account-azureDevOps.yaml
    ├── account-billing.md
    ├── account-billing.yaml
    ├── account-nacls.yaml
    ├── account-resourcePolicies.yaml
    ├── account-route53.yaml
    ├── account-sftpServer.yaml
    ├── account-vgw.yaml
    ├── account-vpcSubnets.yaml
    ├── environment-appflow.yaml
    ├── environment-dynamodb.yaml
    ├── environment-ecr.yaml
    ├── environment-kms.yaml
    ├── environment-s3.yaml
    └── environment-sftp-user.yaml

For integration specific templates, there should be no prefix and just camel case, eg:

    cfn/templates/demo
    ├── README.md
    ├── apigateway.yaml
    ├── fileIngestion.yaml
    ├── hello.yaml
    └── helloGlue.yaml