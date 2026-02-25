# Creating the Virtual Gateway

Each account needs 1 virtual gateway. This gateway is used to attach the direct connect
link and allow network communication from AWS back to SWM. This should only be created
once per account. If the virtual gateway is deleted and recreated it will receive a new ID
and SWM media will need to attach the direct connect link again.

Step 1. Deploying the virtual gateway appliance
   - Confirm the VPC has been created and the VPC ID is in the parameter store
   - Manually run the account-vgw cf template from the templates/platform folder.
   - Reach out to SWM for them to configure the direct connect link and attach it to 
   the VGW you have just created.
