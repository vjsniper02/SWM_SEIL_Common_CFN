# This is basically a full copycat of https://github.com/aws-cloudformation/aws-cloudformation-macros/blob/master
# /Count/src/index.py with minor modifications to fit the use case and added extra comments.
import copy
import json
import re


def process_template(template):
    new_template = copy.deepcopy(template)
    status = "success"

    for name, resource in template["Resources"].items():
        if "AppFlowMultiply" in resource:
            # Get the object names to multiply
            object_names = new_template["Resources"][name].pop("AppFlowMultiply")
            print(
                "Found 'AppFlowMultiply' property with value {} in '{}' resource....multiplying!".format(
                    object_names, name
                )
            )
            # Remove the original resource from the template but take a local copy of it
            resource_to_multiply = new_template["Resources"].pop(name)
            # Create a new block of the resource multiplied with names ending in the iterator and the placeholders
            # substituted
            resources_after_multiplication = multiply(
                name, resource_to_multiply, object_names
            )
            if not set(resources_after_multiplication.keys()) & set(
                new_template["Resources"].keys()
            ):
                new_template["Resources"].update(resources_after_multiplication)
            else:
                status = "failed"
                return status, template
        else:
            print(
                "Did not find 'Count' property in '{}' resource....Nothing to do!".format(
                    name
                )
            )
    return status, new_template


def update_placeholder(
    resource_structure, display_name, object_name, resource_name_suffix
):
    # Convert the json into a string
    resource_string = json.dumps(resource_structure)

    # Replace %flowName. Note: A valid flowName is a combination of alphanumeric characters and the following special characters: !@#.-_
    place_holder_count = resource_string.count(r"%flowName")
    if place_holder_count > 0:
        flowName = re.sub(r"[^a-zA-Z0-9!@#\._]", "-", display_name)
        print(f"Replacing %flowName with {flowName}")
        resource_string = resource_string.replace(r"%flowName", flowName)

    # Replace %objectName. Expect those to be the exact Object name in Salesforce, therefore no processing is needed
    place_holder_count = resource_string.count(r"%objectName")
    if place_holder_count > 0:
        print(f"Replacing %objectName with {object_name}")
        resource_string = resource_string.replace(r"%objectName", object_name)

    # Replace %resourceName. This is the CloudFormation template resource name, they can only be alphanumeric
    place_holder_count = resource_string.count(r"%resourceName")
    if place_holder_count > 0:
        print(f"Replacing %resourceName with {resource_name_suffix}")
        resource_string = resource_string.replace(
            r"%resourceName", resource_name_suffix
        )

    # Convert the string back to json and return it
    return json.loads(resource_string)


def multiply(resource_name, resource_structure, object_names):
    resources = {}
    # Loop through the names of the objects we want to multiply, creating a new resource each time
    names = object_names.split(",")
    for iteration in names:
        # Parse display_name and object_name from this iteration
        print("Multiplying '{}', Object name is {}".format(resource_name, iteration))
        name_parts = iteration.split("|")
        display_name = name_parts[0]
        object_name = name_parts[1] if len(name_parts) > 1 else display_name
        # Strip non alphanumeric characters to meet CFN resource name requirement
        resource_name_suffix = re.sub(r"[^a-zA-Z0-9]", "", display_name)

        multiplied_resource_structure = update_placeholder(
            resource_structure, display_name, object_name, resource_name_suffix
        )
        resources[resource_name + resource_name_suffix] = multiplied_resource_structure
    return resources


def handler(event, context):
    result = process_template(event["fragment"])
    return {
        "requestId": event["requestId"],
        "status": result[0],
        "fragment": result[1],
    }
