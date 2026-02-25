import string
import sys
from xml.dom.minicompat import StringTypes
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

# Script generated for node Account
Account_node1655377496859 = glueContext.create_dynamic_frame.from_catalog(
    database="valid-sf-db",
    table_name="salesforce_account",
    transformation_ctx="Account_node1655377496859",
)

# Script generated for node Order
Order_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="valid-sf-db",
    table_name="salesforce_order",
    transformation_ctx="Order_node1",
)

# Script generated for node RenamedAccount
RenamedAccount_node1655379091827 = ApplyMapping.apply(
    frame=Account_node1655377496859,
    mappings=[
        ("id", "string", "`(a) id`", "string"),
        ("isdeleted", "string", "`(a) isdeleted`", "string"),
        ("masterrecordid", "string", "`(a) masterrecordid`", "string"),
        ("name", "string", "`(a) name`", "string"),
        ("type", "string", "`(a) type`", "string"),
        ("recordtypeid", "string", "`(a) recordtypeid`", "string"),
        ("parentid", "string", "`(a) parentid`", "string"),
        ("billingstreet", "string", "`(a) billingstreet`", "string"),
        ("billingcity", "string", "`(a) billingcity`", "string"),
        ("billingstate", "string", "`(a) billingstate`", "string"),
        ("billingpostalcode", "string", "`(a) billingpostalcode`", "string"),
        ("billingcountry", "string", "`(a) billingcountry`", "string"),
        ("billinglatitude", "string", "`(a) billinglatitude`", "string"),
        ("billinglongitude", "string", "`(a) billinglongitude`", "string"),
        ("billinggeocodeaccuracy", "string", "`(a) billinggeocodeaccuracy`", "string"),
        ("billingaddress", "string", "`(a) billingaddress`", "string"),
        ("shippingstreet", "string", "`(a) shippingstreet`", "string"),
        ("shippingcity", "string", "`(a) shippingcity`", "string"),
        ("shippingstate", "string", "`(a) shippingstate`", "string"),
        ("shippingpostalcode", "string", "`(a) shippingpostalcode`", "string"),
        ("shippingcountry", "string", "`(a) shippingcountry`", "string"),
        ("shippinglatitude", "string", "`(a) shippinglatitude`", "string"),
        ("shippinglongitude", "string", "`(a) shippinglongitude`", "string"),
        (
            "shippinggeocodeaccuracy",
            "string",
            "`(a) shippinggeocodeaccuracy`",
            "string",
        ),
        ("shippingaddress", "string", "`(a) shippingaddress`", "string"),
        ("phone", "string", "`(a) phone`", "string"),
        ("website", "string", "`(a) website`", "string"),
        ("photourl", "string", "`(a) photourl`", "string"),
        ("industry", "string", "`(a) industry`", "string"),
        ("numberofemployees", "string", "`(a) numberofemployees`", "string"),
        ("description", "string", "`(a) description`", "string"),
        ("ownerid", "string", "`(a) ownerid`", "string"),
        ("createddate", "string", "`(a) createddate`", "string"),
        ("createdbyid", "string", "`(a) createdbyid`", "string"),
        ("lastmodifieddate", "string", "`(a) lastmodifieddate`", "string"),
        ("lastmodifiedbyid", "string", "`(a) lastmodifiedbyid`", "string"),
        ("systemmodstamp", "string", "`(a) systemmodstamp`", "string"),
        ("lastactivitydate", "string", "`(a) lastactivitydate`", "string"),
        ("lastvieweddate", "string", "`(a) lastvieweddate`", "string"),
        ("lastreferenceddate", "string", "`(a) lastreferenceddate`", "string"),
        ("jigsaw", "string", "`(a) jigsaw`", "string"),
        ("jigsawcompanyid", "string", "`(a) jigsawcompanyid`", "string"),
        ("accountsource", "string", "`(a) accountsource`", "string"),
        ("sicdesc", "string", "`(a) sicdesc`", "string"),
        (
            "vlocity_cmt__accountpaymenttype__c",
            "string",
            "`(a) vlocity_cmt__accountpaymenttype__c`",
            "string",
        ),
        ("vlocity_cmt__active__c", "string", "`(a) vlocity_cmt__active__c`", "string"),
        (
            "vlocity_cmt__autopaymentamount__c",
            "string",
            "`(a) vlocity_cmt__autopaymentamount__c`",
            "string",
        ),
        (
            "vlocity_cmt__autopaymentcardtype__c",
            "string",
            "`(a) vlocity_cmt__autopaymentcardtype__c`",
            "string",
        ),
        (
            "vlocity_cmt__autopaymentlimitamount__c",
            "string",
            "`(a) vlocity_cmt__autopaymentlimitamount__c`",
            "string",
        ),
        (
            "vlocity_cmt__autopaymentmethodid__c",
            "string",
            "`(a) vlocity_cmt__autopaymentmethodid__c`",
            "string",
        ),
        (
            "vlocity_cmt__billcycle__c",
            "string",
            "`(a) vlocity_cmt__billcycle__c`",
            "string",
        ),
        (
            "vlocity_cmt__billdeliverymethod__c",
            "string",
            "`(a) vlocity_cmt__billdeliverymethod__c`",
            "string",
        ),
        (
            "vlocity_cmt__billformat__c",
            "string",
            "`(a) vlocity_cmt__billformat__c`",
            "string",
        ),
        (
            "vlocity_cmt__billfrequency__c",
            "string",
            "`(a) vlocity_cmt__billfrequency__c`",
            "string",
        ),
        (
            "vlocity_cmt__billnumberofcopies__c",
            "string",
            "`(a) vlocity_cmt__billnumberofcopies__c`",
            "string",
        ),
        (
            "vlocity_cmt__billingaccountstatus__c",
            "string",
            "`(a) vlocity_cmt__billingaccountstatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__billingemailaddress__c",
            "string",
            "`(a) vlocity_cmt__billingemailaddress__c`",
            "string",
        ),
        ("vlocity_cmt__cltv__c", "string", "`(a) vlocity_cmt__cltv__c`", "string"),
        (
            "vlocity_cmt__calculatedaddress__c",
            "string",
            "`(a) vlocity_cmt__calculatedaddress__c`",
            "string",
        ),
        ("vlocity_cmt__churn__c", "string", "`(a) vlocity_cmt__churn__c`", "string"),
        (
            "vlocity_cmt__contactpreferences__c",
            "string",
            "`(a) vlocity_cmt__contactpreferences__c`",
            "string",
        ),
        (
            "vlocity_cmt__creditrating__c",
            "string",
            "`(a) vlocity_cmt__creditrating__c`",
            "string",
        ),
        (
            "vlocity_cmt__creditscore__c",
            "string",
            "`(a) vlocity_cmt__creditscore__c`",
            "string",
        ),
        (
            "vlocity_cmt__customerclass__c",
            "string",
            "`(a) vlocity_cmt__customerclass__c`",
            "string",
        ),
        (
            "vlocity_cmt__customerofbrand__c",
            "string",
            "`(a) vlocity_cmt__customerofbrand__c`",
            "string",
        ),
        (
            "vlocity_cmt__customerpriority__c",
            "string",
            "`(a) vlocity_cmt__customerpriority__c`",
            "string",
        ),
        (
            "vlocity_cmt__customersincedate__c",
            "string",
            "`(a) vlocity_cmt__customersincedate__c`",
            "string",
        ),
        (
            "vlocity_cmt__customervalue__c",
            "string",
            "`(a) vlocity_cmt__customervalue__c`",
            "string",
        ),
        (
            "vlocity_cmt__datefounded__c",
            "string",
            "`(a) vlocity_cmt__datefounded__c`",
            "string",
        ),
        (
            "vlocity_cmt__directorylisted__c",
            "string",
            "`(a) vlocity_cmt__directorylisted__c`",
            "string",
        ),
        (
            "vlocity_cmt__disclosure1__c",
            "string",
            "`(a) vlocity_cmt__disclosure1__c`",
            "string",
        ),
        (
            "vlocity_cmt__disclosure2__c",
            "string",
            "`(a) vlocity_cmt__disclosure2__c`",
            "string",
        ),
        (
            "vlocity_cmt__disclosure3__c",
            "string",
            "`(a) vlocity_cmt__disclosure3__c`",
            "string",
        ),
        (
            "vlocity_cmt__enableautopay__c",
            "string",
            "`(a) vlocity_cmt__enableautopay__c`",
            "string",
        ),
        (
            "vlocity_cmt__fraudreason__c",
            "string",
            "`(a) vlocity_cmt__fraudreason__c`",
            "string",
        ),
        (
            "vlocity_cmt__hasfraud__c",
            "string",
            "`(a) vlocity_cmt__hasfraud__c`",
            "string",
        ),
        (
            "vlocity_cmt__ispersonaccount__c",
            "string",
            "`(a) vlocity_cmt__ispersonaccount__c`",
            "string",
        ),
        (
            "vlocity_cmt__isrootresolved__c",
            "string",
            "`(a) vlocity_cmt__isrootresolved__c`",
            "string",
        ),
        (
            "vlocity_cmt__juridsiction1__c",
            "string",
            "`(a) vlocity_cmt__juridsiction1__c`",
            "string",
        ),
        (
            "vlocity_cmt__jurisdiction2__c",
            "string",
            "`(a) vlocity_cmt__jurisdiction2__c`",
            "string",
        ),
        (
            "vlocity_cmt__legalform__c",
            "string",
            "`(a) vlocity_cmt__legalform__c`",
            "string",
        ),
        (
            "vlocity_cmt__networth__c",
            "string",
            "`(a) vlocity_cmt__networth__c`",
            "string",
        ),
        (
            "vlocity_cmt__numberoflocations__c",
            "string",
            "`(a) vlocity_cmt__numberoflocations__c`",
            "string",
        ),
        (
            "vlocity_cmt__partyid__c",
            "string",
            "`(a) vlocity_cmt__partyid__c`",
            "string",
        ),
        (
            "vlocity_cmt__personcontactid__c",
            "string",
            "`(a) vlocity_cmt__personcontactid__c`",
            "string",
        ),
        (
            "vlocity_cmt__preferredlanguage__c",
            "string",
            "`(a) vlocity_cmt__preferredlanguage__c`",
            "string",
        ),
        (
            "vlocity_cmt__premisesid__c",
            "string",
            "`(a) vlocity_cmt__premisesid__c`",
            "string",
        ),
        (
            "vlocity_cmt__prepayreloadthreshold__c",
            "string",
            "`(a) vlocity_cmt__prepayreloadthreshold__c`",
            "string",
        ),
        (
            "vlocity_cmt__primarycontactid__c",
            "string",
            "`(a) vlocity_cmt__primarycontactid__c`",
            "string",
        ),
        (
            "vlocity_cmt__rootaccountid__c",
            "string",
            "`(a) vlocity_cmt__rootaccountid__c`",
            "string",
        ),
        ("vlocity_cmt__sla__c", "string", "`(a) vlocity_cmt__sla__c`", "string"),
        ("vlocity_cmt__status__c", "string", "`(a) vlocity_cmt__status__c`", "string"),
        (
            "vlocity_cmt__taxexemptionenddate__c",
            "string",
            "`(a) vlocity_cmt__taxexemptionenddate__c`",
            "string",
        ),
        (
            "vlocity_cmt__taxexemptionpercentage__c",
            "string",
            "`(a) vlocity_cmt__taxexemptionpercentage__c`",
            "string",
        ),
        (
            "vlocity_cmt__taxexemptionstartdate__c",
            "string",
            "`(a) vlocity_cmt__taxexemptionstartdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__taxexemptiontype__c",
            "string",
            "`(a) vlocity_cmt__taxexemptiontype__c`",
            "string",
        ),
        ("vlocity_cmt__taxid__c", "string", "`(a) vlocity_cmt__taxid__c`", "string"),
        (
            "vlocity_cmt__upsellopportunity__c",
            "string",
            "`(a) vlocity_cmt__upsellopportunity__c`",
            "string",
        ),
        (
            "vlocity_cmt__vcustomerpriority__c",
            "string",
            "`(a) vlocity_cmt__vcustomerpriority__c`",
            "string",
        ),
        (
            "vlocity_cmt__vslaexpirationdate__c",
            "string",
            "`(a) vlocity_cmt__vslaexpirationdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__vslaserialnumber__c",
            "string",
            "`(a) vlocity_cmt__vslaserialnumber__c`",
            "string",
        ),
        ("vlocity_cmt__vsla__c", "string", "`(a) vlocity_cmt__vsla__c`", "string"),
        ("filename", "string", "`(a) filename`", "string"),
        ("function_name", "string", "`(a) function_name`", "string"),
        ("function_version", "string", "`(a) function_version`", "string"),
        ("aws_request_id", "string", "`(a) aws_request_id`", "string"),
        ("appflow_id", "string", "`(a) appflow_id`", "string"),
        ("processed_ts", "timestamp", "`(a) processed_ts`", "timestamp"),
        ("swm_abn__c", "string", "`(a) swm_abn__c`", "string"),
        ("swm_agency_type__c", "string", "`(a) swm_agency_type__c`", "string"),
        ("swm_billing_agency__c", "string", "`(a) swm_billing_agency__c`", "string"),
        (
            "swm_broadcast_sales_group__c",
            "string",
            "`(a) swm_broadcast_sales_group__c`",
            "string",
        ),
        (
            "swm_business_manager_gsm__c",
            "string",
            "`(a) swm_business_manager_gsm__c`",
            "string",
        ),
        (
            "swm_campaign_manager_cm_team__c",
            "string",
            "`(a) swm_campaign_manager_cm_team__c`",
            "string",
        ),
        (
            "swm_convergent_sales_group__c",
            "string",
            "`(a) swm_convergent_sales_group__c`",
            "string",
        ),
        ("swm_currency__c", "string", "`(a) swm_currency__c`", "string"),
        (
            "swm_digital_sales_group__c",
            "string",
            "`(a) swm_digital_sales_group__c`",
            "string",
        ),
        ("swm_end_date__c", "string", "`(a) swm_end_date__c`", "string"),
        (
            "swm_estimated_turnover__c",
            "string",
            "`(a) swm_estimated_turnover__c`",
            "string",
        ),
        ("swm_loa_attached__c", "string", "`(a) swm_loa_attached__c`", "string"),
        ("swm_planning_agency__c", "string", "`(a) swm_planning_agency__c`", "string"),
        (
            "swm_sales_coord_account_coord_sms__c",
            "string",
            "`(a) swm_sales_coord_account_coord_sms__c`",
            "string",
        ),
        (
            "swm_sales_coord_account_coord__c",
            "string",
            "`(a) swm_sales_coord_account_coord__c`",
            "string",
        ),
        (
            "swm_sales_exec_account_manager__c",
            "string",
            "`(a) swm_sales_exec_account_manager__c`",
            "string",
        ),
        (
            "swm_sales_group_effective_from__c",
            "string",
            "`(a) swm_sales_group_effective_from__c`",
            "string",
        ),
        (
            "swm_sales_group_effective_to__c",
            "string",
            "`(a) swm_sales_group_effective_to__c`",
            "string",
        ),
        ("swm_sales_office__c", "string", "`(a) swm_sales_office__c`", "string"),
        ("swm_start_date__c", "string", "`(a) swm_start_date__c`", "string"),
        ("swm_tax_country__c", "string", "`(a) swm_tax_country__c`", "string"),
        ("swm_taxed__c", "string", "`(a) swm_taxed__c`", "string"),
        ("swm_tier__c", "string", "`(a) swm_tier__c`", "string"),
        ("status__c", "string", "`(a) status__c`", "string"),
        ("swm_partner_account__c", "string", "`(a) swm_partner_account__c`", "string"),
        ("trading_name__c", "string", "`(a) trading_name__c`", "string"),
        ("swm_debtor_id__c", "string", "`(a) swm_debtor_id__c`", "string"),
        ("swm_account_status__c", "string", "`(a) swm_account_status__c`", "string"),
        (
            "swm_activation_status__c",
            "string",
            "`(a) swm_activation_status__c`",
            "string",
        ),
        (
            "swm_account_category_industry__c",
            "string",
            "`(a) swm_account_category_industry__c`",
            "string",
        ),
        (
            "swm_seven_digital_rate_card_linked__c",
            "string",
            "`(a) swm_seven_digital_rate_card_linked__c`",
            "string",
        ),
        ("cci_account_id__c", "string", "`(a) cci_account_id__c`", "string"),
        (
            "swm_wan_digital_rate_card_linked__c",
            "string",
            "`(a) swm_wan_digital_rate_card_linked__c`",
            "string",
        ),
        ("swm_office_address__c", "string", "`(a) swm_office_address__c`", "string"),
        (
            "swm_agency_commission__c",
            "string",
            "`(a) swm_agency_commission__c`",
            "string",
        ),
        (
            "swm_payment_terms_days__c",
            "string",
            "`(a) swm_payment_terms_days__c`",
            "string",
        ),
        (
            "swm_invoicing_preference__c",
            "string",
            "`(a) swm_invoicing_preference__c`",
            "string",
        ),
        (
            "swm_invoices_per_email__c",
            "string",
            "`(a) swm_invoices_per_email__c`",
            "string",
        ),
        (
            "swm_affidavit_required__c",
            "string",
            "`(a) swm_affidavit_required__c`",
            "string",
        ),
        (
            "swm_exemption_number__c",
            "string",
            "`(a) swm_exemption_number__c`",
            "string",
        ),
        ("swm_billing_group__c", "string", "`(a) swm_billing_group__c`", "string"),
        ("agency_discount__c", "string", "`(a) agency_discount__c`", "string"),
        ("cci_customer_id__c", "string", "`(a) cci_customer_id__c`", "string"),
        ("credit_limit__c", "string", "`(a) credit_limit__c`", "string"),
        ("swm_commission_type__c", "string", "`(a) swm_commission_type__c`", "string"),
        (
            "swm_billing_period_type__c",
            "string",
            "`(a) swm_billing_period_type__c`",
            "string",
        ),
        ("partition_0", "string", "`(a) partition_0`", "string"),
        ("partition_1", "string", "`(a) partition_1`", "string"),
        ("partition_2", "string", "`(a) partition_2`", "string"),
        ("partition_3", "string", "`(a) partition_3`", "string"),
    ],
    transformation_ctx="RenamedAccount_node1655379091827",
)

# Script generated for node Join
Join_node1655379071555 = Join.apply(
    frame1=Order_node1,
    frame2=RenamedAccount_node1655379091827,
    keys1=["accountid"],
    keys2=["`(a) id`"],
    transformation_ctx="Join_node1655379071555",
)

# Script generated for node Apply Mapping
ApplyMapping_node1655379163826 = ApplyMapping.apply(
    frame=Join_node1655379071555,
    mappings=[
        ("id", "string", "Sales_Order_ID", "string"),
        (
            "vlocity_cmt__masterordername__c",
            "string",
            "Sales_Order_Name",
            "string",
        ),
        ("accountid", "string", "Account_ID", "string"),
        ("`(a) name`", "string", "Account_Name","string"),
        ("`(a)SWM_Planning_Agency__c`","string", "Functional_Role_Name","string"),
        ],
    transformation_ctx="ApplyMapping_node1655379163826",
)

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = ApplyMapping_node1655379163826.toDF().coalesce(1)

# Added columns not available in Saleforce
partitioned_dataframe = partitioned_dataframe \
    .withColumn("Functional_Role_Internal_Name",lit(None).cast('string')) \
    .withColumn("Functional_Role_ID",lit(None).cast('string')) \
    .withColumn("OP1_Sales_Order_ID",lit(None)) \
    .withColumn("OP1_Account_ID",lit(None)) \
    .withColumn("OP1_Functional_Role_ID",lit(None))

# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

# Script generated for node Amazon S3
Synth_Sales_Order_Accounts_Map = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://swmi-etldev-sf-to-dw-dwmock",
        "partitionKeys": [],
    },
    transformation_ctx="Synth_Sales_Order_Accounts_Map",
)

job.commit()

###############################################################
## Clowning the existing file and deleting the old file.    ###
###############################################################

bucket_name = "swmi-etldev-sf-to-dw-dwmock"
filename = "Synth-Sales_Order_Accounts_Map" + "_" + str(datetime.now().strftime("%Y-%m-%d %H%M%S"))

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