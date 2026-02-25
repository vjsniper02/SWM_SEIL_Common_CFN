### Cloudformation stacks
There are three stacks for creating AppFlow resources.
1. An account-wide ECR stack, `cfn/templates/platform/account-ecr.yaml` creates ECR repositories needed to host the Lambda function images. As the time of writing, this stack is only used by AppFlow utilities Lambdas, although this could change in the future.
1. An account-wide utilities stack, `cfn/templates/platform/account-appflow-utilities.yaml` creates a CFN Macro which will multiply the Flow resource, and a Lambda function for activating/deactivating the Flows. This is because for each Object in Salesforce, we need to create a Flow for it. Instead of manually duplicating the resources in the template, we use a Macro to make it cleaner. We use the Lambda function to automatically activate/deactivate the Flows. This stack is dependent on the ECR stack.
1. The flows stack, `cfn/templates/sf-to-dw/appflow.yaml` creates the Flows and custom resources to automatically activate the Flows once they are created.

### Pipelines for deploying the stacks
1. The ECR stack can be deployed through the `Account` pipeline, via the "Other resources" stage.
1. The Utilities stack can be deployed through the `Account` pipeline, via the "Appflow utility functions" stage.
1. The AppFlow stack can be deployed through the `sf-to-dw` pipeline, via the "Enable Appflows" stage. Before you deploy the AppFlow stack however, you'll need to manually create a Salesforce connector profile via the AWS console and put the connector profile's name into an SSM parameter `/${EnvPrefix}/salesforce/connectorName`.

### Feeding the Object names to the macro
Due to the way macros work, as in the template transformation happens before Cloudformation intrinsic functions are evaluated, we can't use `!Ref ObjectNames` in the `AppFlowMultiply` property to feed object names into the macro. We are going to hardcode the object names into the template since we believe there won't be many of them and all the environments need the same objects.

Each Object name follow the format: `Display Name|[Optional] Object Name` where, the `Display Name` is a human friendly name and the `Object Name` is the Salesforce internal name. e.g. `Account Discount Item|vlocity_cmt__AccountDiscountItem__c`

### Lambda code for the AppflowMultiply macro
The code is basically a full copycat of https://github.com/aws-cloudformation/aws-cloudformation-macros/blob/master/Count/src/index.py with minor modifications to fit the use case and added extra comments.

The code sits in `lambda/utilities/appflowmultiply`.

### Lambda code for FlowActivation
When Cloudformation creates a Flow, the Flow is not activated. Instead of manually activating all the Flows, we use a custom resource to activate them.

Remember that the Flows need to be deactivated before deleting them. Not deactivating the Flows before deleting the Cloudformation stack will cause an error. The Lambda should take care of deactivating the Flows as well.

The code sits in `lambda/utilities/activateflows`.

### Customising a Flow's field mappings
Note that custom field mapping requires A LOT of extra work, this is why we simply map all fields automatically in the template resource definition.

Refer to AWS example for an example of customising field mappings: https://github.com/aws-samples/amazon-appflow/tree/master/CF-template

### Salesforce Connector Profile
Manually create this connector profile per environment by:
1. Logout of Salesforce if you have logged in.
1. Go to the AWS AppFlow console => Connections => Salesforce from the drop down menu => Create connection (a new window is opened)
1. Select use custom domain in the login window, login with username and password
1. Allow access.