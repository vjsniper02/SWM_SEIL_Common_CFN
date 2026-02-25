import boto3
from crhelper import CfnResource

helper = CfnResource()


@helper.create
def create(event, __):
    name = event["ResourceProperties"]["FlowName"]
    client = boto3.client("appflow")
    response = client.start_flow(flowName=name)
    print(response)


@helper.update
def no_op(_, __):
    pass


@helper.delete
def delete(event, __):
    name = event["ResourceProperties"]["FlowName"]
    client = boto3.client("appflow")
    response = client.stop_flow(flowName=name)
    print(response)


def handler(event, context):
    helper(event, context)
