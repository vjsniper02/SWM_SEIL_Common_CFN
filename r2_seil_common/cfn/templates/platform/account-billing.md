### account-billing.yaml is a manually deployed template
This won't go through the pipeline as it's rarely going to be updated. It is also to avoid some logistic issues (e.g. copying SSM Params to us-east-1).

### Speaking of us-east-1
`AWS::CUR::ReportDefinition` is only available in us-east-1. This means this stack can __ONLY__ be deployed to us-east-1.

### Command
Replace `AccountPrefix` value, stack name and profile, etc.
`aws cloudformation create-stack --template-body file://account-billing.yaml --parameters ParameterKey=AccountPrefix,ParameterValue=dev --stack-name dev-billing-report --region us-east-1 --profile dev`

### Changing billing report frequency
Changing `TimeUnit` property requires replacement of the billing report resource. On the other hand, `ReportName` is a required property. As such, to change billing report frequency, you need to also change the name property or the name of the resource.