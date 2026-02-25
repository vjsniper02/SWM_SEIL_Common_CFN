import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Custom
import boto3
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AccountAdServer
AccountAdServer_node1656678660201 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_account_ad_server",
    transformation_ctx="AccountAdServer_node1656678660201",
)

# Script generated for node Apply Mapping
ApplyMapping_node1656678695695 = ApplyMapping.apply(
    frame=AccountAdServer_node1656678660201,
    mappings=[
        (
            "vlocity_cmt__adserveradvertiseridentifier__c",
            "string",
            "Production_System_ID",
            "string",
        ),
        ("isdeleted", "string", "Deleted", "string"),
        
        # Generated
        ("id", "string", "id", "string"),        
        ("name", "string", "name", "string"),
        ("createddate", "string", "createddate", "string"),
        ("createdbyid", "string", "createdbyid", "string"),
        ("lastmodifieddate", "string", "lastmodifieddate", "string"),
        ("lastmodifiedbyid", "string", "lastmodifiedbyid", "string"),
        ("systemmodstamp", "string", "systemmodstamp", "string"),
        ("lastactivitydate", "string", "lastactivitydate", "string"),
        (
            "vlocity_cmt__advertiserid__c",
            "string",
            "vlocity_cmt__advertiserid__c",
            "string",
        ),
        (
            "vlocity_cmt__adserverid__c",
            "string",
            "vlocity_cmt__adserverid__c",
            "string",
        ),
        ("filename", "string", "filename", "string"),
        ("function_name", "string", "function_name", "string"),
        ("function_version", "string", "function_version", "string"),
        ("aws_request_id", "string", "aws_request_id", "string"),
        ("appflow_id", "string", "appflow_id", "string"),
        ("processed_ts", "timestamp", "processed_ts", "timestamp"),
        ("partition_0", "string", "partition_0", "string"),
        ("partition_1", "string", "partition_1", "string"),
        ("partition_2", "string", "partition_2", "string"),
        ("partition_3", "string", "partition_3", "string"),
    ],
    transformation_ctx="ApplyMapping_node1656678695695",
)

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = ApplyMapping_node1656678695695.toDF().coalesce(1)

# Added columns not available in Saleforce
partitioned_dataframe = partitioned_dataframe \
            .withColumn("Sales_Order_Programmatic_Guaranteed_ID",lit(None).cast('string')) \
            .withColumn("Sales_Order_ID",lit(None).cast('string')) \
            .withColumn("Buyer_ID",lit(None).cast('string')) \
            .withColumn("Buyer",lit(None).cast('string')) \
            .withColumn("Buyer_External_ID",lit(None).cast('string')) \
            .withColumn("Proposal_ID",lit(None).cast('string')) \
            .withColumn("Proposal_Status",lit(None).cast('string')) \
            .withColumn("Status_Message",lit(None).cast('string')) \
            .withColumn("OP1_Production_System_ID",lit(None)) \
            .withColumn("OP1_Sales_Order_Programmatic_Guaranteed_ID",lit(None)) \
            .withColumn("OP1_Sales_Order_ID",lit(None)) \
            .withColumn("OP1_Buyer_ID",lit(None)) \
            .withColumn("OP1_Buyer_External_ID",lit(None)) \
            .withColumn("OP1_Proposal_ID",lit(None))

# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")


# Script generated for node Amazon S3
Synth_Sales_Order_Programmatic_Guaranteed = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://swmi-etldev-sf-to-dw-dwmock",
        "partitionKeys": [],
    },
    transformation_ctx="Synth_Sales_Order_Programmatic_Guaranteed",
)

job.commit()

###############################################################
## Clowning the existing file and deleting the old file.    ###
###############################################################

bucket_name = "swmi-etldev-sf-to-dw-dwmock"
filename = "Synth-Sales_Order_Programmatic_Guaranteed" + "_" + str(datetime.now().strftime("%Y-%m-%d %H%M%S"))

client = boto3.client('s3')
#getting all the content/file inside the bucket. 
names = client.list_objects_v2(Bucket=bucket_name)["Contents"]

#Find out the file which have part-000* in it's Key
particulars = [name['Key'] for name in names if 'part-r-000' in name['Key']]

particular = particulars[0]
client.copy_object(Bucket=bucket_name, CopySource=bucket_name + "/" + particular, Key=filename + ".csv")
client.delete_object(Bucket=bucket_name, Key=particular)

############################################################
# create a copy to swm-adsales-ordermanagement/salesforce/development/
############################################################
inputFile = filename + '.csv'
s3_resource = boto3.resource('s3')
current_Object = s3_resource.Object(bucket_name, inputFile)

###########################################################
# create a session for destination bucket
###########################################################           
session_destination = boto3.session.Session(aws_access_key_id="[REDACTED_AWS_ACCESS_KEY]",region_name="ap-southeast-2",aws_secret_access_key="[REDACTED_AWS_SECRET_KEY]")

destination_s3_resouce = session_destination.resource('s3')

captured_object = destination_s3_resouce.Object('swm-adsales-ordermanagement', 'salesforce/development/'+inputFile)

# Create replica to destination S3
captured_object.put(Body=current_Object.get()['Body'].read())