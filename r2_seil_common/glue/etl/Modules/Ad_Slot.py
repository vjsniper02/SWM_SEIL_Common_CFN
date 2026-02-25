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

# Script generated for node AdServer
AdServer_node1656849198238 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_ad_server",
    transformation_ctx="AdServer_node1656849198238",
)

# Script generated for node AccountAdServer
AccountAdServer_node1656849126871 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_account_ad_server",
    transformation_ctx="AccountAdServer_node1656849126871",
)

# Script generated for node RenamedAccountAdServer
RenamedAccountAdServer_node1656849292672 = ApplyMapping.apply(
    frame=AdServer_node1656849198238,
    mappings=[
        ("id", "string", "`(AdSrvr) id`", "string"),
        ("ownerid", "string", "`(AdSrvr) ownerid`", "string"),
        ("isdeleted", "string", "`(AdSrvr) isdeleted`", "string"),
        ("name", "string", "`(AdSrvr) name`", "string"),
        ("createddate", "string", "`(AdSrvr) createddate`", "string"),
        ("createdbyid", "string", "`(AdSrvr) createdbyid`", "string"),
        ("lastmodifieddate", "string", "`(AdSrvr) lastmodifieddate`", "string"),
        ("lastmodifiedbyid", "string", "`(AdSrvr) lastmodifiedbyid`", "string"),
        ("systemmodstamp", "string", "`(AdSrvr) systemmodstamp`", "string"),
        ("lastactivitydate", "string", "`(AdSrvr) lastactivitydate`", "string"),
        ("lastvieweddate", "string", "`(AdSrvr) lastvieweddate`", "string"),
        ("lastreferenceddate", "string", "`(AdSrvr) lastreferenceddate`", "string"),
        (
            "vlocity_cmt__adserverapplicationname__c",
            "string",
            "`(AdSrvr) vlocity_cmt__adserverapplicationname__c`",
            "string",
        ),
        (
            "vlocity_cmt__adservernetworkidentifier__c",
            "string",
            "`(AdSrvr) vlocity_cmt__adservernetworkidentifier__c`",
            "string",
        ),
        (
            "vlocity_cmt__allowedadprioritytypes__c",
            "string",
            "`(AdSrvr) vlocity_cmt__allowedadprioritytypes__c`",
            "string",
        ),
        (
            "vlocity_cmt__namedcredentialreference__c",
            "string",
            "`(AdSrvr) vlocity_cmt__namedcredentialreference__c`",
            "string",
        ),
        ("swm_is_gam__c", "string", "`(AdSrvr) swm_is_gam__c`", "string"),
        ("filename", "string", "`(AdSrvr) filename`", "string"),
        ("function_name", "string", "`(AdSrvr) function_name`", "string"),
        ("function_version", "string", "`(AdSrvr) function_version`", "string"),
        ("aws_request_id", "string", "`(AdSrvr) aws_request_id`", "string"),
        ("appflow_id", "string", "`(AdSrvr) appflow_id`", "string"),
        ("processed_ts", "timestamp", "`(AdSrvr) processed_ts`", "timestamp"),
        ("partition_0", "string", "`(AdSrvr) partition_0`", "string"),
        ("partition_1", "string", "`(AdSrvr) partition_1`", "string"),
        ("partition_2", "string", "`(AdSrvr) partition_2`", "string"),
        ("partition_3", "string", "`(AdSrvr) partition_3`", "string"),
    ],
    transformation_ctx="RenamedAccountAdServer_node1656849292672",
)

# Script generated for node AccAdServerJoinsAdServer
AccAdServerJoinsAdServer_node1656849233208 = Join.apply(
    frame1=AccountAdServer_node1656849126871,
    frame2=RenamedAccountAdServer_node1656849292672,
    keys1=["vlocity_cmt__adserverid__c"],
    keys2=["`(AdSrvr) id`"],
    transformation_ctx="AccAdServerJoinsAdServer_node1656849233208",
)

# Script generated for node Apply Mapping
ApplyMapping_node1656849428189 = ApplyMapping.apply(
    frame=AccAdServerJoinsAdServer_node1656849233208,
    mappings=[
        (
            "vlocity_cmt__adserverid__c",
            "string",
            "Ad_Slot_ID",
            "string",
        ),
        ("name", "string", "Ad_Slot_Name", "string"),
        (
            "vlocity_cmt__adserveradvertiseridentifier__c",
            "string",
            "Production_System_ID",
            "string",
        ),
        ("`(AdSrvr) name`", "string", "Production_System_Name", "string"),
        (
            "`(AdSrvr) vlocity_cmt__adserverapplicationname__c`",
            "string",
            "Media_Property_Name",
            "string",
        ),

        #Generated
        ("id", "string", "id", "string"),
        ("isdeleted", "string", "isdeleted", "string"),
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
        ("`(AdSrvr) id`", "string", "`(AdSrvr) id`", "string"),
        ("`(AdSrvr) ownerid`", "string", "`(AdSrvr) ownerid`", "string"),
        ("`(AdSrvr) isdeleted`", "string", "`(AdSrvr) isdeleted`", "string"),
        ("`(AdSrvr) createddate`", "string", "`(AdSrvr) createddate`", "string"),
        ("`(AdSrvr) createdbyid`", "string", "`(AdSrvr) createdbyid`", "string"),
        (
            "`(AdSrvr) lastmodifieddate`",
            "string",
            "`(AdSrvr) lastmodifieddate`",
            "string",
        ),
        (
            "`(AdSrvr) lastmodifiedbyid`",
            "string",
            "`(AdSrvr) lastmodifiedbyid`",
            "string",
        ),
        ("`(AdSrvr) systemmodstamp`", "string", "`(AdSrvr) systemmodstamp`", "string"),
        (
            "`(AdSrvr) lastactivitydate`",
            "string",
            "`(AdSrvr) lastactivitydate`",
            "string",
        ),
        ("`(AdSrvr) lastvieweddate`", "string", "`(AdSrvr) lastvieweddate`", "string"),
        (
            "`(AdSrvr) lastreferenceddate`",
            "string",
            "`(AdSrvr) lastreferenceddate`",
            "string",
        ),
        (
            "`(AdSrvr) vlocity_cmt__adservernetworkidentifier__c`",
            "string",
            "`(AdSrvr) vlocity_cmt__adservernetworkidentifier__c`",
            "string",
        ),
        (
            "`(AdSrvr) vlocity_cmt__allowedadprioritytypes__c`",
            "string",
            "`(AdSrvr) vlocity_cmt__allowedadprioritytypes__c`",
            "string",
        ),
        (
            "`(AdSrvr) vlocity_cmt__namedcredentialreference__c`",
            "string",
            "`(AdSrvr) vlocity_cmt__namedcredentialreference__c`",
            "string",
        ),
        ("`(AdSrvr) swm_is_gam__c`", "string", "`(AdSrvr) swm_is_gam__c`", "string"),
    ],
    transformation_ctx="ApplyMapping_node1656849428189",
)

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = ApplyMapping_node1656849428189.toDF().coalesce(1)

# Added columns not available in Saleforce
partitioned_dataframe = partitioned_dataframe \
            .withColumn("Forecast_Source_ID",lit(None)) \
            .withColumn("Media_Property_ID",lit(None)) \
            .withColumn("Ad_Slot_Status",lit(None)) \
            .withColumn("Locator_1",lit(None)) \
            .withColumn("Locator_2",lit(None)) \
            .withColumn("Locator_3",lit(None)) \
            .withColumn("Locator_4",lit(None)) \
            .withColumn("Locator_5",lit(None)) \
            .withColumn("Special_Instructions",lit(None)) \
            .withColumn("Payment_Term_ID",lit(None)) \
            .withColumn("Payment_Term_Name",lit(None)) \
            .withColumn("Locator_1_external_ID",lit(None)) \
            .withColumn("Locator_2_external_ID",lit(None)) \
            .withColumn("Locator_3_external_ID",lit(None)) \
            .withColumn("Locator_4_external_ID",lit(None)) \
            .withColumn("Locator_5_external_ID",lit(None)) \
            .withColumn("MDSP_ID",lit(None)) \
            .withColumn("MDSP_Name",lit(None)) \
            .withColumn("OP1_Ad_Slot_ID",lit(None)) \
            .withColumn("OP1_Production_System_ID",lit(None)) \
            .withColumn("OP1_Forecast_Source_ID",lit(None)) \
            .withColumn("OP1_Media_Property_ID",lit(None)) \
            .withColumn("OP1_Locator_1",lit(None)) \
            .withColumn("OP1_Locator_2",lit(None)) \
            .withColumn("OP1_Locator_3",lit(None)) \
            .withColumn("OP1_Locator_4",lit(None)) \
            .withColumn("OP1_Locator_5",lit(None)) \
            .withColumn("OP1_Payment_Term_ID",lit(None)) \
            .withColumn("OP1_Locator_1_external_ID",lit(None)) \
            .withColumn("OP1_Locator_2_external_ID",lit(None)) \
            .withColumn("OP1_Locator_3_external_ID",lit(None)) \
            .withColumn("OP1_Locator_4_external_ID",lit(None)) \
            .withColumn("OP1_Locator_5_external_ID",lit(None)) \
            .withColumn("OP1_MDSP_ID",lit(None))
            
# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

# Script generated for node Amazon S3
Synth_Ad_Slot = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://swmi-etldev-sf-to-dw-dwmock",
        "partitionKeys": [],
    },
    transformation_ctx="Synth_Ad_Slot",
)

job.commit()

###############################################################
## Clowning the existing file and deleting the old file.    ###
###############################################################

bucket_name = "swmi-etldev-sf-to-dw-dwmock"
filename = "Synth-Ad_Slot" + "_" + str(datetime.now().strftime("%Y-%m-%d %H%M%S"))

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
