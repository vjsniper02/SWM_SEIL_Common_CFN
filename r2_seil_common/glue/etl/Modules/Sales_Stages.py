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

# Script generated for node Opportunity
Opportunity_node1660299984711 = glueContext.create_dynamic_frame.from_catalog(
    database="test-sf-db",
    table_name="salesforce_opportunity",
    transformation_ctx="Opportunity_node1660299984711",
)

# Script generated for node Apply Mapping
ApplyMapping_node1660300346295 = ApplyMapping.apply(
    frame=Opportunity_node1660299984711,
    mappings=[
        ("stagename", "string", "Sales_Stage_Name", "string"),
        ("probability", "string", "Sales_Stage_Percent", "string"),
        ("swm_status__c", "string", "Status", "string"),
        ("isdeleted", "string", "Deleted", "string"),
        ("createdbyid", "string", "Created_By_ID", "string"),
        ("createddate", "string", "Created_On", "string"),
        ("lastmodifiedbyid", "string", "Last_Modified_By_ID", "string"),
        ("lastmodifieddate", "string", "Last_Modified_On", "string"),        
        ## Generated
        # ("id", "string", "id", "string"),        
        # ("accountid", "string", "accountid", "string"),
        # ("recordtypeid", "string", "recordtypeid", "string"),
        # ("name", "string", "name", "string"),
        # ("description", "string", "description", "string"),
        # ("amount", "string", "amount", "string"),        
        # ("closedate", "string", "closedate", "string"),
        # ("type", "string", "type", "string"),
        # ("nextstep", "string", "nextstep", "string"),
        # ("leadsource", "string", "leadsource", "string"),
        # ("isclosed", "string", "isclosed", "string"),
        # ("iswon", "string", "iswon", "string"),
        # ("forecastcategory", "string", "forecastcategory", "string"),
        # ("forecastcategoryname", "string", "forecastcategoryname", "string"),
        # ("campaignid", "string", "campaignid", "string"),
        # ("hasopportunitylineitem", "string", "hasopportunitylineitem", "string"),
        # ("pricebook2id", "string", "pricebook2id", "string"),
        # ("ownerid", "string", "ownerid", "string"),        
        # ("systemmodstamp", "string", "systemmodstamp", "string"),
        # ("lastactivitydate", "string", "lastactivitydate", "string"),
        # ("fiscalquarter", "string", "fiscalquarter", "string"),
        # ("fiscalyear", "string", "fiscalyear", "string"),
        # ("fiscal", "string", "fiscal", "string"),
        # ("contactid", "string", "contactid", "string"),
        # ("lastvieweddate", "string", "lastvieweddate", "string"),
        # ("lastreferenceddate", "string", "lastreferenceddate", "string"),
        # ("syncedquoteid", "string", "syncedquoteid", "string"),
        # ("contractid", "string", "contractid", "string"),
        # ("hasopenactivity", "string", "hasopenactivity", "string"),
        # ("hasoverduetask", "string", "hasoverduetask", "string"),
        # (
        #     "lastamountchangedhistoryid",
        #     "string",
        #     "lastamountchangedhistoryid",
        #     "string",
        # ),
        # (
        #     "lastclosedatechangedhistoryid",
        #     "string",
        #     "lastclosedatechangedhistoryid",
        #     "string",
        # ),
        # ("budget_confirmed__c", "string", "budget_confirmed__c", "string"),
        # ("discovery_completed__c", "string", "discovery_completed__c", "string"),
        # ("roi_analysis_completed__c", "string", "roi_analysis_completed__c", "string"),
        # (
        #     "vlocity_cmt__accountrecordtype__c",
        #     "string",
        #     "vlocity_cmt__accountrecordtype__c",
        #     "string",
        # ),
        # ("loss_reason__c", "string", "loss_reason__c", "string"),
        # (
        #     "vlocity_cmt__accountsla__c",
        #     "string",
        #     "vlocity_cmt__accountsla__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__asyncjobid__c",
        #     "string",
        #     "vlocity_cmt__asyncjobid__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__asyncoperation__c",
        #     "string",
        #     "vlocity_cmt__asyncoperation__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__budgetamount__c",
        #     "string",
        #     "vlocity_cmt__budgetamount__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__defaultcurrencypaymentmode__c",
        #     "string",
        #     "vlocity_cmt__defaultcurrencypaymentmode__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__effectivebaseonetimecharge__c",
        #     "string",
        #     "vlocity_cmt__effectivebaseonetimecharge__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__effectivebaserecurringcharge__c",
        #     "string",
        #     "vlocity_cmt__effectivebaserecurringcharge__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__effectiveonetimecosttotal__c",
        #     "string",
        #     "vlocity_cmt__effectiveonetimecosttotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__effectiveonetimeloyaltytotal__c",
        #     "string",
        #     "vlocity_cmt__effectiveonetimeloyaltytotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__effectiveopportunitytotal__c",
        #     "string",
        #     "vlocity_cmt__effectiveopportunitytotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__effectiverecurringcosttotal__c",
        #     "string",
        #     "vlocity_cmt__effectiverecurringcosttotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__effectiveusagecosttotal__c",
        #     "string",
        #     "vlocity_cmt__effectiveusagecosttotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__effectiveusagepricetotal__c",
        #     "string",
        #     "vlocity_cmt__effectiveusagepricetotal__c",
        #     "string",
        # ),
        # ("vlocity_cmt__email__c", "string", "vlocity_cmt__email__c", "string"),
        # ("vlocity_cmt__fax__c", "string", "vlocity_cmt__fax__c", "string"),
        # (
        #     "vlocity_cmt__followupto__c",
        #     "string",
        #     "vlocity_cmt__followupto__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__framecontractid__c",
        #     "string",
        #     "vlocity_cmt__framecontractid__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__iscontractrequired__c",
        #     "string",
        #     "vlocity_cmt__iscontractrequired__c",
        #     "string",
        # ),
        # ("vlocity_cmt__lockedby__c", "string", "vlocity_cmt__lockedby__c", "string"),
        # ("vlocity_cmt__lockedfor__c", "string", "vlocity_cmt__lockedfor__c", "string"),
        # (
        #     "vlocity_cmt__numberofcontractedmonths__c",
        #     "string",
        #     "vlocity_cmt__numberofcontractedmonths__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__onetimeloyaltytotal__c",
        #     "string",
        #     "vlocity_cmt__onetimeloyaltytotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__onetimemargintotal__c",
        #     "string",
        #     "vlocity_cmt__onetimemargintotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__opportunitymargintotal__c",
        #     "string",
        #     "vlocity_cmt__opportunitymargintotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__opportunitytotal__c",
        #     "string",
        #     "vlocity_cmt__opportunitytotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__ordernumber__c",
        #     "string",
        #     "vlocity_cmt__ordernumber__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__originatingchannel__c",
        #     "string",
        #     "vlocity_cmt__originatingchannel__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__originatingcontractid__c",
        #     "string",
        #     "vlocity_cmt__originatingcontractid__c",
        #     "string",
        # ),
        # ("vlocity_cmt__partyid__c", "string", "vlocity_cmt__partyid__c", "string"),
        # ("vlocity_cmt__phone__c", "string", "vlocity_cmt__phone__c", "string"),
        # (
        #     "vlocity_cmt__pricelistid__c",
        #     "string",
        #     "vlocity_cmt__pricelistid__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__recurringmargintotal__c",
        #     "string",
        #     "vlocity_cmt__recurringmargintotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__shippingpostalcode__c",
        #     "string",
        #     "vlocity_cmt__shippingpostalcode__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__shippingstate__c",
        #     "string",
        #     "vlocity_cmt__shippingstate__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__trackingnumber__c",
        #     "string",
        #     "vlocity_cmt__trackingnumber__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__usagemargintotal__c",
        #     "string",
        #     "vlocity_cmt__usagemargintotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__validationdate__c",
        #     "string",
        #     "vlocity_cmt__validationdate__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__validationmessage__c",
        #     "string",
        #     "vlocity_cmt__validationmessage__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__validationstatus__c",
        #     "string",
        #     "vlocity_cmt__validationstatus__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__effectiveonetimetotal__c",
        #     "string",
        #     "vlocity_cmt__effectiveonetimetotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__effectiverecurringtotal__c",
        #     "string",
        #     "vlocity_cmt__effectiverecurringtotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__onetimetotal__c",
        #     "string",
        #     "vlocity_cmt__onetimetotal__c",
        #     "string",
        # ),
        # (
        #     "vlocity_cmt__recurringtotal__c",
        #     "string",
        #     "vlocity_cmt__recurringtotal__c",
        #     "string",
        # ),
        # ("swm_activity_code__c", "string", "swm_activity_code__c", "string"),
        # ("swm_agency_commissions__c", "string", "swm_agency_commissions__c", "string"),
        # ("swm_agency__c", "string", "swm_agency__c", "string"),
        # ("swm_billing_agency__c", "string", "swm_billing_agency__c", "string"),
        # ("swm_billing_type__c", "string", "swm_billing_type__c", "string"),
        # ("swm_campaign_objective__c", "string", "swm_campaign_objective__c", "string"),
        # ("swm_campaign_type__c", "string", "swm_campaign_type__c", "string"),
        # ("swm_currency__c", "string", "swm_currency__c", "string"),
        # ("swm_end_date__c", "string", "swm_end_date__c", "string"),
        # ("swm_high_value__c", "string", "swm_high_value__c", "string"),
        # ("swm_overall_budget__c", "string", "swm_overall_budget__c", "string"),
        # ("swm_planning_agency__c", "string", "swm_planning_agency__c", "string"),
        # (
        #     "swm_specify_campaign_objective__c",
        #     "string",
        #     "swm_specify_campaign_objective__c",
        #     "string",
        # ),
        # ("swm_start_date__c", "string", "swm_start_date__c", "string"),
        # ("swm_total_barter_value__c", "string", "swm_total_barter_value__c", "string"),
        # ("agency_group__c", "string", "agency_group__c", "string"),
        # (
        #     "swm_total_broadcast_make_good_value__c",
        #     "string",
        #     "swm_total_broadcast_make_good_value__c",
        #     "string",
        # ),
        # ("swm_total_compensation__c", "string", "swm_total_compensation__c", "string"),
        # ("swm_total_contra__c", "string", "swm_total_contra__c", "string"),
        # ("swm_total_credit_memos__c", "string", "swm_total_credit_memos__c", "string"),
        # ("swm_total_credit__c", "string", "swm_total_credit__c", "string"),
        # (
        #     "swm_total_gross_minus_credit__c",
        #     "string",
        #     "swm_total_gross_minus_credit__c",
        #     "string",
        # ),
        # ("swm_total_gross__c", "string", "swm_total_gross__c", "string"),
        # ("swm_total_impressions__c", "string", "swm_total_impressions__c", "string"),
        # (
        #     "swm_total_investment_contra__c",
        #     "string",
        #     "swm_total_investment_contra__c",
        #     "string",
        # ),
        # ("swm_total_line_items__c", "string", "swm_total_line_items__c", "string"),
        # (
        #     "swm_total_make_good_value__c",
        #     "string",
        #     "swm_total_make_good_value__c",
        #     "string",
        # ),
        # (
        #     "swm_total_net_minus_credit__c",
        #     "string",
        #     "swm_total_net_minus_credit__c",
        #     "string",
        # ),
        # ("swm_total_net__c", "string", "swm_total_net__c", "string"),
        # ("swm_ecpm__c", "string", "swm_ecpm__c", "string"),
        # ("brief_id__c", "string", "brief_id__c", "string"),
        # ("io_ref_no__c", "string", "io_ref_no__c", "string"),
        # ("swm_total_bonus_value__c", "string", "swm_total_bonus_value__c", "string"),
        # ("swm_approval_status__c", "string", "swm_approval_status__c", "string"),
        # ("swm_floor_rate_total__c", "string", "swm_floor_rate_total__c", "string"),
        # ("swm_market_rate_total__c", "string", "swm_market_rate_total__c", "string"),
        # ("swm_one_time_total__c", "string", "swm_one_time_total__c", "string"),
        # (
        #     "swm_count_checked_on_quote__c",
        #     "string",
        #     "swm_count_checked_on_quote__c",
        #     "string",
        # ),
        # (
        #     "swm_count_gam_line_on_quote__c",
        #     "string",
        #     "swm_count_gam_line_on_quote__c",
        #     "string",
        # ),
        # (
        #     "swm_can_be_sent_for_approval__c",
        #     "string",
        #     "swm_can_be_sent_for_approval__c",
        #     "string",
        # ),
        # ("filename", "string", "filename", "string"),
        # ("function_name", "string", "function_name", "string"),
        # ("function_version", "string", "function_version", "string"),
        # ("aws_request_id", "string", "aws_request_id", "string"),
        # ("appflow_id", "string", "appflow_id", "string"),
        # ("processed_ts", "timestamp", "processed_ts", "timestamp"),
        # ("total_no_of_quotes__c", "string", "total_no_of_quotes__c", "string"),
        # (
        #     "swm_credit_limit_reached_formula__c",
        #     "string",
        #     "swm_credit_limit_reached_formula__c",
        #     "string",
        # ),
        # (
        #     "swm_credit_limit_breached__c",
        #     "string",
        #     "swm_credit_limit_breached__c",
        #     "string",
        # ),
        # ("revenue_type__c", "string", "revenue_type__c", "string"),
        # ("credit_limit_breached__c", "string", "credit_limit_breached__c", "string"),
        # (
        #     "credit_limit_breached_formula__c",
        #     "string",
        #     "credit_limit_breached_formula__c",
        #     "string",
        # ),
        # (
        #     "count_quote_on_opportunity__c",
        #     "string",
        #     "count_quote_on_opportunity__c",
        #     "string",
        # ),
        # ("swm_creative_contact__c", "string", "swm_creative_contact__c", "string"),
        # ("partition_0", "string", "partition_0", "string"),
        # ("partition_1", "string", "partition_1", "string"),
        # ("partition_2", "string", "partition_2", "string"),
        # ("partition_3", "string", "partition_3", "string"),
    ],
    transformation_ctx="ApplyMapping_node1660300346295",
)

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = ApplyMapping_node1660300346295.toDF().coalesce(1)

# Added columns not available in Saleforce
partitioned_dataframe = partitioned_dataframe \
    .withColumn("Sales_Stage_ID",lit(None)) \
    .withColumn("Sort_Order",lit(None)) \
    .withColumn("Allow_Order_Create",lit(None)) \
    .withColumn("CRM_System_ID",lit(None)) \
    .withColumn("CRM_System_Name",lit(None)) \
    .withColumn("External_ID",lit(None)) \
    .withColumn("Native",lit(None)) \
    .withColumn("Created_By",lit(None)) \
    .withColumn("Last_Modified_By",lit(None)) \
    .withColumn("OP1_Sales_Stage_ID",lit(None)) \
    .withColumn("OP1_CRM_System_ID",lit(None)) \
    .withColumn("OP1_External_ID",lit(None)) \
    .withColumn("OP1_Created_By_ID",lit(None)) \
    .withColumn("OP1_Last_Modified_By_ID",lit(None))

# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

# Script generated for node Amazon S3
Synth_Sales_Stages = glueContext.getSink(
    path="s3://swmi-etldev-sf-to-dw-dwmock",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Synth_Sales_Stages",
)
Synth_Sales_Stages.setCatalogInfo(
    catalogDatabase="test-sf-db", catalogTableName="salesforce_opportunity"
)
Synth_Sales_Stages.setFormat("csv")
Synth_Sales_Stages.writeFrame(partitioned_dynamicframe)
job.commit()

###############################################################
## Clowning the existing file and deleting the old file.    ###
###############################################################

bucket_name = "swmi-etldev-sf-to-dw-dwmock"
filename = "Synth-Sales_Stages" + "_" + str(datetime.now().strftime("%d%m%Y"))

client = boto3.client('s3')
#getting all the content/file inside the bucket. 
names = client.list_objects_v2(Bucket=bucket_name)["Contents"]

#Find out the file which have part-000* in it's Key
particulars = [name['Key'] for name in names if 'part-r-000' in name['Key']]

particular = particulars[0]
client.copy_object(Bucket=bucket_name, CopySource=bucket_name + "/" + particular, Key=filename + ".csv")
client.delete_object(Bucket=bucket_name, Key=particular)