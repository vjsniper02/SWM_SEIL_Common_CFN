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

# Script generated for node Order
Order_node1655462134742 = glueContext.create_dynamic_frame.from_catalog(
    database="test-sf-db",
    table_name="salesforce_order",
    transformation_ctx="Order_node1655462134742",
)

# Script generated for node Quote
Quote_node1655462183490 = glueContext.create_dynamic_frame.from_catalog(
    database="test-sf-db",
    table_name="salesforce_quote",
    transformation_ctx="Quote_node1655462183490",
)

# Script generated for node RenamedQuote
RenamedQuote_node1655462268259 = ApplyMapping.apply(
    frame=Quote_node1655462183490,
    mappings=[
        ("id", "string", "`(q) id`", "string"),
        ("ownerid", "string", "`(q) ownerid`", "string"),
        ("isdeleted", "string", "`(q) isdeleted`", "string"),
        ("name", "string", "`(q) name`", "string"),
        ("recordtypeid", "string", "`(q) recordtypeid`", "string"),
        ("createddate", "string", "`(q) createddate`", "string"),
        ("createdbyid", "string", "`(q) createdbyid`", "string"),
        ("lastmodifieddate", "string", "`(q) lastmodifieddate`", "string"),
        ("lastmodifiedbyid", "string", "`(q) lastmodifiedbyid`", "string"),
        ("systemmodstamp", "string", "`(q) systemmodstamp`", "string"),
        ("lastvieweddate", "string", "`(q) lastvieweddate`", "string"),
        ("lastreferenceddate", "string", "`(q) lastreferenceddate`", "string"),
        ("opportunityid", "string", "`(q) opportunityid`", "string"),
        ("pricebook2id", "string", "`(q) pricebook2id`", "string"),
        ("contactid", "string", "`(q) contactid`", "string"),
        ("quotenumber", "string", "`(q) quotenumber`", "string"),
        ("issyncing", "string", "`(q) issyncing`", "string"),
        ("shippinghandling", "string", "`(q) shippinghandling`", "string"),
        ("tax", "string", "`(q) tax`", "string"),
        ("status", "string", "`(q) status`", "string"),
        ("expirationdate", "string", "`(q) expirationdate`", "string"),
        ("description", "string", "`(q) description`", "string"),
        ("subtotal", "string", "`(q) subtotal`", "string"),
        ("totalprice", "string", "`(q) totalprice`", "string"),
        ("lineitemcount", "string", "`(q) lineitemcount`", "string"),
        ("billingstreet", "string", "`(q) billingstreet`", "string"),
        ("billingcity", "string", "`(q) billingcity`", "string"),
        ("billingstate", "string", "`(q) billingstate`", "string"),
        ("billingpostalcode", "string", "`(q) billingpostalcode`", "string"),
        ("billingcountry", "string", "`(q) billingcountry`", "string"),
        ("billinglatitude", "string", "`(q) billinglatitude`", "string"),
        ("billinglongitude", "string", "`(q) billinglongitude`", "string"),
        ("billinggeocodeaccuracy", "string", "`(q) billinggeocodeaccuracy`", "string"),
        ("billingaddress", "string", "`(q) billingaddress`", "string"),
        ("shippingstreet", "string", "`(q) shippingstreet`", "string"),
        ("shippingcity", "string", "`(q) shippingcity`", "string"),
        ("shippingstate", "string", "`(q) shippingstate`", "string"),
        ("shippingpostalcode", "string", "`(q) shippingpostalcode`", "string"),
        ("shippingcountry", "string", "`(q) shippingcountry`", "string"),
        ("shippinglatitude", "string", "`(q) shippinglatitude`", "string"),
        ("shippinglongitude", "string", "`(q) shippinglongitude`", "string"),
        (
            "shippinggeocodeaccuracy",
            "string",
            "`(q) shippinggeocodeaccuracy`",
            "string",
        ),
        ("shippingaddress", "string", "`(q) shippingaddress`", "string"),
        ("quotetostreet", "string", "`(q) quotetostreet`", "string"),
        ("quotetocity", "string", "`(q) quotetocity`", "string"),
        ("quotetostate", "string", "`(q) quotetostate`", "string"),
        ("quotetopostalcode", "string", "`(q) quotetopostalcode`", "string"),
        ("quotetocountry", "string", "`(q) quotetocountry`", "string"),
        ("quotetolatitude", "string", "`(q) quotetolatitude`", "string"),
        ("quotetolongitude", "string", "`(q) quotetolongitude`", "string"),
        ("quotetogeocodeaccuracy", "string", "`(q) quotetogeocodeaccuracy`", "string"),
        ("quotetoaddress", "string", "`(q) quotetoaddress`", "string"),
        ("additionalstreet", "string", "`(q) additionalstreet`", "string"),
        ("additionalcity", "string", "`(q) additionalcity`", "string"),
        ("additionalstate", "string", "`(q) additionalstate`", "string"),
        ("additionalpostalcode", "string", "`(q) additionalpostalcode`", "string"),
        ("additionalcountry", "string", "`(q) additionalcountry`", "string"),
        ("additionallatitude", "string", "`(q) additionallatitude`", "string"),
        ("additionallongitude", "string", "`(q) additionallongitude`", "string"),
        (
            "additionalgeocodeaccuracy",
            "string",
            "`(q) additionalgeocodeaccuracy`",
            "string",
        ),
        ("additionaladdress", "string", "`(q) additionaladdress`", "string"),
        ("billingname", "string", "`(q) billingname`", "string"),
        ("shippingname", "string", "`(q) shippingname`", "string"),
        ("quotetoname", "string", "`(q) quotetoname`", "string"),
        ("additionalname", "string", "`(q) additionalname`", "string"),
        ("email", "string", "`(q) email`", "string"),
        ("phone", "string", "`(q) phone`", "string"),
        ("fax", "string", "`(q) fax`", "string"),
        ("contractid", "string", "`(q) contractid`", "string"),
        ("accountid", "string", "`(q) accountid`", "string"),
        ("discount", "string", "`(q) discount`", "string"),
        ("grandtotal", "string", "`(q) grandtotal`", "string"),
        (
            "cancreatequotelineitems",
            "string",
            "`(q) cancreatequotelineitems`",
            "string",
        ),
        (
            "vlocity_cmt__accountrecordtype__c",
            "string",
            "`(q) vlocity_cmt__accountrecordtype__c`",
            "string",
        ),
        (
            "vlocity_cmt__accountsla__c",
            "string",
            "`(q) vlocity_cmt__accountsla__c`",
            "string",
        ),
        (
            "vlocity_cmt__asyncjobid__c",
            "string",
            "`(q) vlocity_cmt__asyncjobid__c`",
            "string",
        ),
        (
            "vlocity_cmt__asyncoperation__c",
            "string",
            "`(q) vlocity_cmt__asyncoperation__c`",
            "string",
        ),
        (
            "vlocity_cmt__budgetamount__c",
            "string",
            "`(q) vlocity_cmt__budgetamount__c`",
            "string",
        ),
        (
            "vlocity_cmt__campaignid__c",
            "string",
            "`(q) vlocity_cmt__campaignid__c`",
            "string",
        ),
        (
            "vlocity_cmt__creditcheckdecision__c",
            "string",
            "`(q) vlocity_cmt__creditcheckdecision__c`",
            "string",
        ),
        (
            "vlocity_cmt__defaultbillingaccountid__c",
            "string",
            "`(q) vlocity_cmt__defaultbillingaccountid__c`",
            "string",
        ),
        (
            "vlocity_cmt__defaultcurrencypaymentmode__c",
            "string",
            "`(q) vlocity_cmt__defaultcurrencypaymentmode__c`",
            "string",
        ),
        (
            "vlocity_cmt__defaultpremisesid__c",
            "string",
            "`(q) vlocity_cmt__defaultpremisesid__c`",
            "string",
        ),
        (
            "vlocity_cmt__defaultserviceaccountid__c",
            "string",
            "`(q) vlocity_cmt__defaultserviceaccountid__c`",
            "string",
        ),
        (
            "vlocity_cmt__defaultservicepointid__c",
            "string",
            "`(q) vlocity_cmt__defaultservicepointid__c`",
            "string",
        ),
        (
            "vlocity_cmt__deliveryinstallationstatus__c",
            "string",
            "`(q) vlocity_cmt__deliveryinstallationstatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectivebaseonetimecharge__c",
            "string",
            "`(q) vlocity_cmt__effectivebaseonetimecharge__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectivebaserecurringcharge__c",
            "string",
            "`(q) vlocity_cmt__effectivebaserecurringcharge__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveonetimecosttotal__c",
            "string",
            "`(q) vlocity_cmt__effectiveonetimecosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveonetimeloyaltytotal__c",
            "string",
            "`(q) vlocity_cmt__effectiveonetimeloyaltytotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectivequotetotal__c",
            "string",
            "`(q) vlocity_cmt__effectivequotetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiverecurringcosttotal__c",
            "string",
            "`(q) vlocity_cmt__effectiverecurringcosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveusagecosttotal__c",
            "string",
            "`(q) vlocity_cmt__effectiveusagecosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveusagepricetotal__c",
            "string",
            "`(q) vlocity_cmt__effectiveusagepricetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__externalpricingstatus__c",
            "string",
            "`(q) vlocity_cmt__externalpricingstatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__framecontractid__c",
            "string",
            "`(q) vlocity_cmt__framecontractid__c`",
            "string",
        ),
        (
            "vlocity_cmt__iscontractrequired__c",
            "string",
            "`(q) vlocity_cmt__iscontractrequired__c`",
            "string",
        ),
        (
            "vlocity_cmt__ispriced__c",
            "string",
            "`(q) vlocity_cmt__ispriced__c`",
            "string",
        ),
        (
            "vlocity_cmt__isvalidated__c",
            "string",
            "`(q) vlocity_cmt__isvalidated__c`",
            "string",
        ),
        (
            "vlocity_cmt__lastpricedat__c",
            "string",
            "`(q) vlocity_cmt__lastpricedat__c`",
            "string",
        ),
        (
            "vlocity_cmt__leadsource__c",
            "string",
            "`(q) vlocity_cmt__leadsource__c`",
            "string",
        ),
        (
            "vlocity_cmt__lockedby__c",
            "string",
            "`(q) vlocity_cmt__lockedby__c`",
            "string",
        ),
        (
            "vlocity_cmt__lockedfor__c",
            "string",
            "`(q) vlocity_cmt__lockedfor__c`",
            "string",
        ),
        (
            "vlocity_cmt__multiservicemessagedata__c",
            "string",
            "`(q) vlocity_cmt__multiservicemessagedata__c`",
            "string",
        ),
        (
            "vlocity_cmt__numberofcontractedmonths__c",
            "string",
            "`(q) vlocity_cmt__numberofcontractedmonths__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimeloyaltytotal__c",
            "string",
            "`(q) vlocity_cmt__onetimeloyaltytotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimemargintotal__c",
            "string",
            "`(q) vlocity_cmt__onetimemargintotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimetotal2__c",
            "string",
            "`(q) vlocity_cmt__onetimetotal2__c`",
            "string",
        ),
        (
            "vlocity_cmt__opportunityaccountid__c",
            "string",
            "`(q) vlocity_cmt__opportunityaccountid__c`",
            "string",
        ),
        (
            "vlocity_cmt__originatingchannel__c",
            "string",
            "`(q) vlocity_cmt__originatingchannel__c`",
            "string",
        ),
        (
            "vlocity_cmt__parentquoteid__c",
            "string",
            "`(q) vlocity_cmt__parentquoteid__c`",
            "string",
        ),
        ("vlocity_cmt__podate__c", "string", "`(q) vlocity_cmt__podate__c`", "string"),
        (
            "vlocity_cmt__ponumber__c",
            "string",
            "`(q) vlocity_cmt__ponumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__pricelistid__c",
            "string",
            "`(q) vlocity_cmt__pricelistid__c`",
            "string",
        ),
        (
            "vlocity_cmt__quotegroupid__c",
            "string",
            "`(q) vlocity_cmt__quotegroupid__c`",
            "string",
        ),
        (
            "vlocity_cmt__quotemargintotal__c",
            "string",
            "`(q) vlocity_cmt__quotemargintotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__quotetotal__c",
            "string",
            "`(q) vlocity_cmt__quotetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringmargintotal__c",
            "string",
            "`(q) vlocity_cmt__recurringmargintotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringtotal2__c",
            "string",
            "`(q) vlocity_cmt__recurringtotal2__c`",
            "string",
        ),
        (
            "vlocity_cmt__renewalsourcecontractid__c",
            "string",
            "`(q) vlocity_cmt__renewalsourcecontractid__c`",
            "string",
        ),
        (
            "vlocity_cmt__shippingpostalcode__c",
            "string",
            "`(q) vlocity_cmt__shippingpostalcode__c`",
            "string",
        ),
        (
            "vlocity_cmt__shippingstate__c",
            "string",
            "`(q) vlocity_cmt__shippingstate__c`",
            "string",
        ),
        (
            "vlocity_cmt__syncedorderid__c",
            "string",
            "`(q) vlocity_cmt__syncedorderid__c`",
            "string",
        ),
        (
            "vlocity_cmt__trackingnumber__c",
            "string",
            "`(q) vlocity_cmt__trackingnumber__c`",
            "string",
        ),
        ("vlocity_cmt__type__c", "string", "`(q) vlocity_cmt__type__c`", "string"),
        (
            "vlocity_cmt__usagemargintotal__c",
            "string",
            "`(q) vlocity_cmt__usagemargintotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__validationdate__c",
            "string",
            "`(q) vlocity_cmt__validationdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__validationmessage__c",
            "string",
            "`(q) vlocity_cmt__validationmessage__c`",
            "string",
        ),
        (
            "vlocity_cmt__validationstatus__c",
            "string",
            "`(q) vlocity_cmt__validationstatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveonetimetotal__c",
            "string",
            "`(q) vlocity_cmt__effectiveonetimetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiverecurringtotal__c",
            "string",
            "`(q) vlocity_cmt__effectiverecurringtotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimetotal__c",
            "string",
            "`(q) vlocity_cmt__onetimetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringtotal__c",
            "string",
            "`(q) vlocity_cmt__recurringtotal__c`",
            "string",
        ),
        (
            "swm_discounting_exception__c",
            "string",
            "`(q) swm_discounting_exception__c`",
            "string",
        ),
        ("reason__c", "string", "`(q) reason__c`", "string"),
        ("swm_primary__c", "string", "`(q) swm_primary__c`", "string"),
        (
            "swm_bonus_product_value__c",
            "string",
            "`(q) swm_bonus_product_value__c`",
            "string",
        ),
        (
            "swm_baseonetimetotal__c",
            "string",
            "`(q) swm_baseonetimetotal__c`",
            "string",
        ),
        ("swm_one_time_total__c", "string", "`(q) swm_one_time_total__c`", "string"),
        ("swm_related_quote__c", "string", "`(q) swm_related_quote__c`", "string"),
        ("approval_status__c", "string", "`(q) approval_status__c`", "string"),
        (
            "swm_high_value_bonus_product__c",
            "string",
            "`(q) swm_high_value_bonus_product__c`",
            "string",
        ),
        (
            "swm_market_rate_total__c",
            "string",
            "`(q) swm_market_rate_total__c`",
            "string",
        ),
        (
            "swm_floor_rate_total__c",
            "string",
            "`(q) swm_floor_rate_total__c`",
            "string",
        ),
        ("swm_rate_card__c", "string", "`(q) swm_rate_card__c`", "string"),
        ("swm_source_quote__c", "string", "`(q) swm_source_quote__c`", "string"),
        (
            "swm_inventory_validation__c",
            "string",
            "`(q) swm_inventory_validation__c`",
            "string",
        ),
        (
            "inventory_status_history__c",
            "string",
            "`(q) inventory_status_history__c`",
            "string",
        ),
        (
            "inventory_availability__c",
            "string",
            "`(q) inventory_availability__c`",
            "string",
        ),
        ("swm_gam_lines__c", "string", "`(q) swm_gam_lines__c`", "string"),
        ("swm_non_gam_lines__c", "string", "`(q) swm_non_gam_lines__c`", "string"),
        (
            "swm_contains_gam_lines__c",
            "string",
            "`(q) swm_contains_gam_lines__c`",
            "string",
        ),
        (
            "swm_contains_non_gam_lines__c",
            "string",
            "`(q) swm_contains_non_gam_lines__c`",
            "string",
        ),
        ("filename", "string", "`(q) filename`", "string"),
        ("function_name", "string", "`(q) function_name`", "string"),
        ("function_version", "string", "`(q) function_version`", "string"),
        ("aws_request_id", "string", "`(q) aws_request_id`", "string"),
        ("appflow_id", "string", "`(q) appflow_id`", "string"),
        ("processed_ts", "timestamp", "`(q) processed_ts`", "timestamp"),
        (
            "contains_only_non_gam_lines__c",
            "string",
            "`(q) contains_only_non_gam_lines__c`",
            "string",
        ),
        ("loggedin_user__c", "string", "`(q) loggedin_user__c`", "string"),
        ("partition_0", "string", "`(q) partition_0`", "string"),
        ("partition_1", "string", "`(q) partition_1`", "string"),
        ("partition_2", "string", "`(q) partition_2`", "string"),
        ("partition_3", "string", "`(q) partition_3`", "string"),
    ],
    transformation_ctx="RenamedQuote_node1655462268259",
)

# Script generated for node Join
Join_node1655462241965 = Join.apply(
    frame1=Order_node1655462134742,
    frame2=RenamedQuote_node1655462268259,
    keys1=["vlocity_cmt__quoteid__c"],
    keys2=["`(q) id`"],
    transformation_ctx="Join_node1655462241965",
)

# Script generated for node Apply Mapping
ApplyMapping_node1655462332725 = ApplyMapping.apply(
    frame=Join_node1655462241965,
    mappings=[
        ("`(q) id`","string", "Sales_Order_Approval_ID","string"),
        ("id", "string", "Sales_Order_ID", "string"),
        ("`(q) description`", "string", "Rule_Description", "string"),
        ("`(q) approval_status__c`", "string","Approval_Status","string"),
        (
            "vlocity_cmt__orderstatus__c",
            "string",
            "Order_Status",
            "string",
        ),
        (
            "`(q) vlocity_cmt__type__c`",
            "string",
            "Approval_Type",
            "string",
        ),
        ("ownerid", "string", "Approver_ID", "string"),
        ("vlocity_cmt__OrderGroupId__c", "string", "Approval_Group_ID", "string"),
        ("`(q) ownerid`", "string", "Approver", "string"),
        ("`(q) vlocity_cmt__lockedby__c`","string","Submitted_By","string"),
        ("activateddate", "string", "Submitted_On", "string"),
        ("lastmodifieddate", "string", "Last_Modified_On", "string"),
        ("lastmodifiedbyid", "string", "Last_Modified_By_ID", "string"),
    ],
    transformation_ctx="ApplyMapping_node1655462332725",
)

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = ApplyMapping_node1655462332725.toDF().coalesce(1)

# Added columns not available in Saleforce
partitioned_dataframe = partitioned_dataframe.withColumn("Rule_ID",lit(None)) \
    .withColumn("Rule_Name",lit(None)) \
    .withColumn("Rule_Is_Incremental",lit(None)) \
    .withColumn("Approval_Note",lit(None)) \
    .withColumn("Submitted_By_ID",lit(None)) \
    .withColumn("Last_Modified_By",lit(None)) \
    .withColumn("OP1_Sales_Order_Approval_ID",lit(None)) \
    .withColumn("OP1_Sales_Order_ID",lit(None)) \
    .withColumn("OP1_Approver_ID",lit(None)) \
    .withColumn("OP1_Approval_Group_ID",lit(None)) \
    .withColumn("OP1_Last_Modified_By_ID",lit(None)) \
    .withColumn("OP1_Rule_ID",lit(None)) \
    .withColumn("OP1_Submitted_By_ID",lit(None))

# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

# Script generated for node Amazon S3
Synth_Sales_Order_Approvals = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://swmi-etldev-sf-to-dw-dwmock",
        "partitionKeys": [],
    },
    transformation_ctx="Synth_Sales_Order_Approvals",
)

job.commit()


###############################################################
## Clowning the existing file and deleting the old file.    ###
###############################################################

bucket_name = "swmi-etldev-sf-to-dw-dwmock"
filename = "Synth-Sales_Order_Approvals" + "_" + str(datetime.now().strftime("%Y-%m-%d %H%M%S"))

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
