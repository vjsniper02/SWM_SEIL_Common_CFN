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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_order",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "string", "Sales_Order_ID", "string"),
        ("type", "string", "Sales_Order_Type", "string"),
        (
            "vlocity_cmt__orderstatus__c",
            "string",
            "Status",
            "string",
        ),
        ("description", "string", "Description", "string"),
        ("pricebook2id", "string", "Price_Book_Name", "string"),
        ("createdbyid", "string", "Created_By_ID", "string"),
        ("createddate", "string", "Created_On", "string"),
        ("lastmodifiedbyid", "string", "Last_Modified_By_ID", "string"),
        ("lastmodifieddate", "string", "Last_Modified_On", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = ApplyMapping_node2.toDF().coalesce(1)

# Added columns not available in Saleforce
partitioned_dataframe = partitioned_dataframe.withColumn("Default",lit(None)) \
                .withColumn("Sort_Order",lit(None)) \
                .withColumn("Auto_Create_In_CRM_System",lit(None)) \
                .withColumn("CRM_System_ID",lit(None)) \
                .withColumn("CRM_System_Name",lit(None)) \
                .withColumn("Default_Sales_Stage_ID",lit(None)) \
                .withColumn("Default_Sales_Stage_Name",lit(None)) \
                .withColumn("Created_By",lit(None)) \
                .withColumn("Last_Modified_By",lit(None)) \
                .withColumn("OP1_Sales_Order_ID",lit(None)) \
                .withColumn("OP1_Created_By_ID",lit(None)) \
                .withColumn("OP1_Last_Modified_By_ID",lit(None)) \
                .withColumn("OP1_CRM_System_ID",lit(None)) \
                .withColumn("OP1_Default_Sales_Stage_ID",lit(None))

# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

# Script generated for node S3 bucket
Synth_Sales_Order_Type = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://swmi-etldev-sf-to-dw-dwmock",
        "partitionKeys": [],
    },
    transformation_ctx="Synth_Sales_Order_Type",
)

job.commit()

###############################################################
## Clowning the existing file and deleting the old file.    ###
###############################################################

bucket_name = "swmi-etldev-sf-to-dw-dwmock"
filename = "Synth-Sales_Order_Type" + "_" + str(datetime.now().strftime("%Y-%m-%d %H%M%S"))

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
