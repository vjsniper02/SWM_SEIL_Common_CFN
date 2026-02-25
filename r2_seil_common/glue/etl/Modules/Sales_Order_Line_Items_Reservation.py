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
Order_node1656582415757 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_order",
    transformation_ctx="Order_node1656582415757",
)

# Script generated for node OrderAdPlacement
OrderAdPlacement_node1656582465354 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_order_ad_placement",
    transformation_ctx="OrderAdPlacement_node1656582465354",
)

# Script generated for node Contract
Contract_node1656582625896 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_contract",
    transformation_ctx="Contract_node1656582625896",
)

# Script generated for node OrderProduct
OrderProduct_node1656582812558 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_order_product",
    transformation_ctx="OrderProduct_node1656582812558",
)

# Script generated for node RenamedOrderAdPlacement
RenamedOrderAdPlacement_node1656582564229 = ApplyMapping.apply(
    frame=OrderAdPlacement_node1656582465354,
    mappings=[
        ("id", "string", "`(oap) id`", "string"),
        ("isdeleted", "string", "`(oap) isdeleted`", "string"),
        ("name", "string", "`(oap) name`", "string"),
        ("createddate", "string", "`(oap) createddate`", "string"),
        ("createdbyid", "string", "`(oap) createdbyid`", "string"),
        ("lastmodifieddate", "string", "`(oap) lastmodifieddate`", "string"),
        ("lastmodifiedbyid", "string", "`(oap) lastmodifiedbyid`", "string"),
        ("systemmodstamp", "string", "`(oap) systemmodstamp`", "string"),
        ("lastactivitydate", "string", "`(oap) lastactivitydate`", "string"),
        (
            "vlocity_cmt__orderid__c",
            "string",
            "`(oap) vlocity_cmt__orderid__c`",
            "string",
        ),
        (
            "vlocity_cmt__adbleedamountuom__c",
            "string",
            "`(oap) vlocity_cmt__adbleedamountuom__c`",
            "string",
        ),
        (
            "vlocity_cmt__adbleedamount__c",
            "string",
            "`(oap) vlocity_cmt__adbleedamount__c`",
            "string",
        ),
        (
            "vlocity_cmt__adcreativesizetypes__c",
            "string",
            "`(oap) vlocity_cmt__adcreativesizetypes__c`",
            "string",
        ),
        (
            "vlocity_cmt__adcreativeurl__c",
            "string",
            "`(oap) vlocity_cmt__adcreativeurl__c`",
            "string",
        ),
        (
            "vlocity_cmt__adplacementprioritytype__c",
            "string",
            "`(oap) vlocity_cmt__adplacementprioritytype__c`",
            "string",
        ),
        (
            "vlocity_cmt__adrequestedenddate__c",
            "string",
            "`(oap) vlocity_cmt__adrequestedenddate__c`",
            "string",
        ),
        (
            "vlocity_cmt__adrequestedstartdate__c",
            "string",
            "`(oap) vlocity_cmt__adrequestedstartdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__adserverorderidentifier__c",
            "string",
            "`(oap) vlocity_cmt__adserverorderidentifier__c`",
            "string",
        ),
        (
            "vlocity_cmt__adserverorderlineidentiifier__c",
            "string",
            "`(oap) vlocity_cmt__adserverorderlineidentiifier__c`",
            "string",
        ),
        (
            "vlocity_cmt__adspacespecificationid__c",
            "string",
            "`(oap) vlocity_cmt__adspacespecificationid__c`",
            "string",
        ),
        (
            "vlocity_cmt__adtimeperepisode__c",
            "string",
            "`(oap) vlocity_cmt__adtimeperepisode__c`",
            "string",
        ),
        (
            "vlocity_cmt__bonusadtime__c",
            "string",
            "`(oap) vlocity_cmt__bonusadtime__c`",
            "string",
        ),
        (
            "vlocity_cmt__costperratingpoint__c",
            "string",
            "`(oap) vlocity_cmt__costperratingpoint__c`",
            "string",
        ),
        (
            "vlocity_cmt__customerdaypart__c",
            "string",
            "`(oap) vlocity_cmt__customerdaypart__c`",
            "string",
        ),
        (
            "vlocity_cmt__grossratingpoint__c",
            "string",
            "`(oap) vlocity_cmt__grossratingpoint__c`",
            "string",
        ),
        (
            "vlocity_cmt__impliedrate__c",
            "string",
            "`(oap) vlocity_cmt__impliedrate__c`",
            "string",
        ),
        (
            "vlocity_cmt__impliedtotal__c",
            "string",
            "`(oap) vlocity_cmt__impliedtotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__isadbleedenabled__c",
            "string",
            "`(oap) vlocity_cmt__isadbleedenabled__c`",
            "string",
        ),
        (
            "vlocity_cmt__maximumfrequencyinterval__c",
            "string",
            "`(oap) vlocity_cmt__maximumfrequencyinterval__c`",
            "string",
        ),
        (
            "vlocity_cmt__maximumfrequency__c",
            "string",
            "`(oap) vlocity_cmt__maximumfrequency__c`",
            "string",
        ),
        (
            "vlocity_cmt__maximumuserfrequencyinterval__c",
            "string",
            "`(oap) vlocity_cmt__maximumuserfrequencyinterval__c`",
            "string",
        ),
        (
            "vlocity_cmt__maximumuserfrequency__c",
            "string",
            "`(oap) vlocity_cmt__maximumuserfrequency__c`",
            "string",
        ),
        (
            "vlocity_cmt__mediatype__c",
            "string",
            "`(oap) vlocity_cmt__mediatype__c`",
            "string",
        ),
        (
            "vlocity_cmt__orderitemid__c",
            "string",
            "`(oap) vlocity_cmt__orderitemid__c`",
            "string",
        ),
        (
            "vlocity_cmt__paidadtime__c",
            "string",
            "`(oap) vlocity_cmt__paidadtime__c`",
            "string",
        ),
        (
            "vlocity_cmt__quotelineitemid__c",
            "string",
            "`(oap) vlocity_cmt__quotelineitemid__c`",
            "string",
        ),
        (
            "vlocity_cmt__sponsorshiptype__c",
            "string",
            "`(oap) vlocity_cmt__sponsorshiptype__c`",
            "string",
        ),
        (
            "vlocity_cmt__targetingparameters__c",
            "string",
            "`(oap) vlocity_cmt__targetingparameters__c`",
            "string",
        ),
        (
            "vlocity_cmt__totaladtime__c",
            "string",
            "`(oap) vlocity_cmt__totaladtime__c`",
            "string",
        ),
        (
            "vlocity_cmt__userengagementgoaltype__c",
            "string",
            "`(oap) vlocity_cmt__userengagementgoaltype__c`",
            "string",
        ),
        (
            "vlocity_cmt__userengagementgoalunittype__c",
            "string",
            "`(oap) vlocity_cmt__userengagementgoalunittype__c`",
            "string",
        ),
        (
            "vlocity_cmt__userengagementgoalunit__c",
            "string",
            "`(oap) vlocity_cmt__userengagementgoalunit__c`",
            "string",
        ),
        ("cost_method__c", "string", "`(oap) cost_method__c`", "string"),
        (
            "swm_parent_media_placement__c",
            "string",
            "`(oap) swm_parent_media_placement__c`",
            "string",
        ),
        ("production_system__c", "string", "`(oap) production_system__c`", "string"),
        ("swm_market_rate__c", "string", "`(oap) swm_market_rate__c`", "string"),
        ("swm_floor_price__c", "string", "`(oap) swm_floor_price__c`", "string"),
        (
            "swm_one_time_charge__c",
            "string",
            "`(oap) swm_one_time_charge__c`",
            "string",
        ),
        ("filename", "string", "`(oap) filename`", "string"),
        ("function_name", "string", "`(oap) function_name`", "string"),
        ("function_version", "string", "`(oap) function_version`", "string"),
        ("aws_request_id", "string", "`(oap) aws_request_id`", "string"),
        ("appflow_id", "string", "`(oap) appflow_id`", "string"),
        ("processed_ts", "timestamp", "`(oap) processed_ts`", "timestamp"),
        ("lastvieweddate", "string", "`(oap) lastvieweddate`", "string"),
        ("lastreferenceddate", "string", "`(oap) lastreferenceddate`", "string"),
        ("swm_sub_status__c", "string", "`(oap) swm_sub_status__c`", "string"),
        ("partition_0", "string", "`(oap) partition_0`", "string"),
        ("partition_1", "string", "`(oap) partition_1`", "string"),
        ("partition_2", "string", "`(oap) partition_2`", "string"),
        ("partition_3", "string", "`(oap) partition_3`", "string"),
    ],
    transformation_ctx="RenamedOrderAdPlacement_node1656582564229",
)

# Script generated for node RenamedContract
RenamedContract_node1656582709130 = ApplyMapping.apply(
    frame=Contract_node1656582625896,
    mappings=[
        ("id", "string", "`(cntr) id`", "string"),
        ("accountid", "string", "`(cntr) accountid`", "string"),
        ("pricebook2id", "string", "`(cntr) pricebook2id`", "string"),
        ("ownerexpirationnotice", "string", "`(cntr) ownerexpirationnotice`", "string"),
        ("startdate", "string", "`(cntr) startdate`", "string"),
        ("enddate", "string", "`(cntr) enddate`", "string"),
        ("billingstreet", "string", "`(cntr) billingstreet`", "string"),
        ("billingcity", "string", "`(cntr) billingcity`", "string"),
        ("billingstate", "string", "`(cntr) billingstate`", "string"),
        ("billingpostalcode", "string", "`(cntr) billingpostalcode`", "string"),
        ("billingcountry", "string", "`(cntr) billingcountry`", "string"),
        ("billinglatitude", "string", "`(cntr) billinglatitude`", "string"),
        ("billinglongitude", "string", "`(cntr) billinglongitude`", "string"),
        (
            "billinggeocodeaccuracy",
            "string",
            "`(cntr) billinggeocodeaccuracy`",
            "string",
        ),
        ("billingaddress", "string", "`(cntr) billingaddress`", "string"),
        ("shippingstreet", "string", "`(cntr) shippingstreet`", "string"),
        ("shippingcity", "string", "`(cntr) shippingcity`", "string"),
        ("shippingstate", "string", "`(cntr) shippingstate`", "string"),
        ("shippingpostalcode", "string", "`(cntr) shippingpostalcode`", "string"),
        ("shippingcountry", "string", "`(cntr) shippingcountry`", "string"),
        ("shippinglatitude", "string", "`(cntr) shippinglatitude`", "string"),
        ("shippinglongitude", "string", "`(cntr) shippinglongitude`", "string"),
        (
            "shippinggeocodeaccuracy",
            "string",
            "`(cntr) shippinggeocodeaccuracy`",
            "string",
        ),
        ("shippingaddress", "string", "`(cntr) shippingaddress`", "string"),
        ("contractterm", "string", "`(cntr) contractterm`", "string"),
        ("ownerid", "string", "`(cntr) ownerid`", "string"),
        ("status", "string", "`(cntr) status`", "string"),
        ("companysignedid", "string", "`(cntr) companysignedid`", "string"),
        ("companysigneddate", "string", "`(cntr) companysigneddate`", "string"),
        ("customersignedid", "string", "`(cntr) customersignedid`", "string"),
        ("customersignedtitle", "string", "`(cntr) customersignedtitle`", "string"),
        ("customersigneddate", "string", "`(cntr) customersigneddate`", "string"),
        ("specialterms", "string", "`(cntr) specialterms`", "string"),
        ("activatedbyid", "string", "`(cntr) activatedbyid`", "string"),
        ("activateddate", "string", "`(cntr) activateddate`", "string"),
        ("statuscode", "string", "`(cntr) statuscode`", "string"),
        ("description", "string", "`(cntr) description`", "string"),
        ("recordtypeid", "string", "`(cntr) recordtypeid`", "string"),
        ("isdeleted", "string", "`(cntr) isdeleted`", "string"),
        ("contractnumber", "string", "`(cntr) contractnumber`", "string"),
        ("lastapproveddate", "string", "`(cntr) lastapproveddate`", "string"),
        ("createddate", "string", "`(cntr) createddate`", "string"),
        ("createdbyid", "string", "`(cntr) createdbyid`", "string"),
        ("lastmodifieddate", "string", "`(cntr) lastmodifieddate`", "string"),
        ("lastmodifiedbyid", "string", "`(cntr) lastmodifiedbyid`", "string"),
        ("systemmodstamp", "string", "`(cntr) systemmodstamp`", "string"),
        ("lastactivitydate", "string", "`(cntr) lastactivitydate`", "string"),
        ("lastvieweddate", "string", "`(cntr) lastvieweddate`", "string"),
        ("lastreferenceddate", "string", "`(cntr) lastreferenceddate`", "string"),
        (
            "vlocity_cmt__activecontractversionid__c",
            "string",
            "`(cntr) vlocity_cmt__activecontractversionid__c`",
            "string",
        ),
        (
            "vlocity_cmt__applicationid__c",
            "string",
            "`(cntr) vlocity_cmt__applicationid__c`",
            "string",
        ),
        (
            "vlocity_cmt__autorenewobjectcreation__c",
            "string",
            "`(cntr) vlocity_cmt__autorenewobjectcreation__c`",
            "string",
        ),
        (
            "vlocity_cmt__contractgroupid__c",
            "string",
            "`(cntr) vlocity_cmt__contractgroupid__c`",
            "string",
        ),
        (
            "vlocity_cmt__contractreferencenumber__c",
            "string",
            "`(cntr) vlocity_cmt__contractreferencenumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__contracttype__c",
            "string",
            "`(cntr) vlocity_cmt__contracttype__c`",
            "string",
        ),
        (
            "vlocity_cmt__expirationreason__c",
            "string",
            "`(cntr) vlocity_cmt__expirationreason__c`",
            "string",
        ),
        (
            "vlocity_cmt__expiredcontractid__c",
            "string",
            "`(cntr) vlocity_cmt__expiredcontractid__c`",
            "string",
        ),
        (
            "vlocity_cmt__isactivedocumentlocked__c",
            "string",
            "`(cntr) vlocity_cmt__isactivedocumentlocked__c`",
            "string",
        ),
        (
            "vlocity_cmt__isactiveversionbatchtemplate__c",
            "string",
            "`(cntr) vlocity_cmt__isactiveversionbatchtemplate__c`",
            "string",
        ),
        (
            "vlocity_cmt__isautorenew__c",
            "string",
            "`(cntr) vlocity_cmt__isautorenew__c`",
            "string",
        ),
        (
            "vlocity_cmt__isframecontract__c",
            "string",
            "`(cntr) vlocity_cmt__isframecontract__c`",
            "string",
        ),
        (
            "vlocity_cmt__isnonstandard__c",
            "string",
            "`(cntr) vlocity_cmt__isnonstandard__c`",
            "string",
        ),
        (
            "vlocity_cmt__lastdocusignenvelopeid__c",
            "string",
            "`(cntr) vlocity_cmt__lastdocusignenvelopeid__c`",
            "string",
        ),
        (
            "vlocity_cmt__lastdocusignenvelopestatus__c",
            "string",
            "`(cntr) vlocity_cmt__lastdocusignenvelopestatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__lastdocusignjobid__c",
            "string",
            "`(cntr) vlocity_cmt__lastdocusignjobid__c`",
            "string",
        ),
        (
            "vlocity_cmt__lastdocumentbatchjoberror__c",
            "string",
            "`(cntr) vlocity_cmt__lastdocumentbatchjoberror__c`",
            "string",
        ),
        (
            "vlocity_cmt__lastdocumentbatchjobid__c",
            "string",
            "`(cntr) vlocity_cmt__lastdocumentbatchjobid__c`",
            "string",
        ),
        (
            "vlocity_cmt__lastdocumentbatchjobstatus__c",
            "string",
            "`(cntr) vlocity_cmt__lastdocumentbatchjobstatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__localecode__c",
            "string",
            "`(cntr) vlocity_cmt__localecode__c`",
            "string",
        ),
        (
            "vlocity_cmt__opportunityid__c",
            "string",
            "`(cntr) vlocity_cmt__opportunityid__c`",
            "string",
        ),
        (
            "vlocity_cmt__orderid__c",
            "string",
            "`(cntr) vlocity_cmt__orderid__c`",
            "string",
        ),
        (
            "vlocity_cmt__originalcontractid__c",
            "string",
            "`(cntr) vlocity_cmt__originalcontractid__c`",
            "string",
        ),
        (
            "vlocity_cmt__parentcontractid__c",
            "string",
            "`(cntr) vlocity_cmt__parentcontractid__c`",
            "string",
        ),
        (
            "vlocity_cmt__pricelistid__c",
            "string",
            "`(cntr) vlocity_cmt__pricelistid__c`",
            "string",
        ),
        (
            "vlocity_cmt__quoteid__c",
            "string",
            "`(cntr) vlocity_cmt__quoteid__c`",
            "string",
        ),
        (
            "vlocity_cmt__renewalnotificationterm__c",
            "string",
            "`(cntr) vlocity_cmt__renewalnotificationterm__c`",
            "string",
        ),
        (
            "vlocity_cmt__renewalnotification__c",
            "string",
            "`(cntr) vlocity_cmt__renewalnotification__c`",
            "string",
        ),
        (
            "vlocity_cmt__renewalstartdate__c",
            "string",
            "`(cntr) vlocity_cmt__renewalstartdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__sendrenewalnotification__c",
            "string",
            "`(cntr) vlocity_cmt__sendrenewalnotification__c`",
            "string",
        ),
        (
            "vlocity_cmt__terminatedate__c",
            "string",
            "`(cntr) vlocity_cmt__terminatedate__c`",
            "string",
        ),
        (
            "vlocity_cmt__terminationreason__c",
            "string",
            "`(cntr) vlocity_cmt__terminationreason__c`",
            "string",
        ),
        (
            "vlocity_cmt__terminationtype__c",
            "string",
            "`(cntr) vlocity_cmt__terminationtype__c`",
            "string",
        ),
        (
            "vlocity_cmt__countofcontractversion__c",
            "string",
            "`(cntr) vlocity_cmt__countofcontractversion__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimetotal__c",
            "string",
            "`(cntr) vlocity_cmt__onetimetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringtotal__c",
            "string",
            "`(cntr) vlocity_cmt__recurringtotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__sumofnonstandardsection__c",
            "string",
            "`(cntr) vlocity_cmt__sumofnonstandardsection__c`",
            "string",
        ),
        (
            "vlocity_cmt__totalmonthlydiscount__c",
            "string",
            "`(cntr) vlocity_cmt__totalmonthlydiscount__c`",
            "string",
        ),
        (
            "vlocity_cmt__totalonetimediscount__c",
            "string",
            "`(cntr) vlocity_cmt__totalonetimediscount__c`",
            "string",
        ),
        (
            "calculationmatrixversion__c",
            "string",
            "`(cntr) calculationmatrixversion__c`",
            "string",
        ),
        ("calculationmatrix__c", "string", "`(cntr) calculationmatrix__c`", "string"),
        ("filename", "string", "`(cntr) filename`", "string"),
        ("function_name", "string", "`(cntr) function_name`", "string"),
        ("function_version", "string", "`(cntr) function_version`", "string"),
        ("aws_request_id", "string", "`(cntr) aws_request_id`", "string"),
        ("appflow_id", "string", "`(cntr) appflow_id`", "string"),
        ("processed_ts", "timestamp", "`(cntr) processed_ts`", "timestamp"),
        ("partition_0", "string", "`(cntr) partition_0`", "string"),
        ("partition_1", "string", "`(cntr) partition_1`", "string"),
        ("partition_2", "string", "`(cntr) partition_2`", "string"),
        ("partition_3", "string", "`(cntr) partition_3`", "string"),
    ],
    transformation_ctx="RenamedContract_node1656582709130",
)

# Script generated for node RenamedOrderProduct
RenamedOrderProduct_node1656582906626 = ApplyMapping.apply(
    frame=OrderProduct_node1656582812558,
    mappings=[
        ("id", "string", "`(op) id`", "string"),
        ("product2id", "string", "`(op) product2id`", "string"),
        ("isdeleted", "string", "`(op) isdeleted`", "string"),
        ("orderid", "string", "`(op) orderid`", "string"),
        ("pricebookentryid", "string", "`(op) pricebookentryid`", "string"),
        ("originalorderitemid", "string", "`(op) originalorderitemid`", "string"),
        ("availablequantity", "string", "`(op) availablequantity`", "string"),
        ("quantity", "string", "`(op) quantity`", "string"),
        ("unitprice", "string", "`(op) unitprice`", "string"),
        ("listprice", "string", "`(op) listprice`", "string"),
        ("totalprice", "string", "`(op) totalprice`", "string"),
        ("servicedate", "string", "`(op) servicedate`", "string"),
        ("enddate", "string", "`(op) enddate`", "string"),
        ("description", "string", "`(op) description`", "string"),
        ("createddate", "string", "`(op) createddate`", "string"),
        ("createdbyid", "string", "`(op) createdbyid`", "string"),
        ("lastmodifieddate", "string", "`(op) lastmodifieddate`", "string"),
        ("lastmodifiedbyid", "string", "`(op) lastmodifiedbyid`", "string"),
        ("systemmodstamp", "string", "`(op) systemmodstamp`", "string"),
        ("orderitemnumber", "string", "`(op) orderitemnumber`", "string"),
        ("vlocity_cmt__action__c", "string", "`(op) vlocity_cmt__action__c`", "string"),
        (
            "vlocity_cmt__activationname__c",
            "string",
            "`(op) vlocity_cmt__activationname__c`",
            "string",
        ),
        (
            "vlocity_cmt__additional_discount__c",
            "string",
            "`(op) vlocity_cmt__additional_discount__c`",
            "string",
        ),
        (
            "vlocity_cmt__assetid__c",
            "string",
            "`(op) vlocity_cmt__assetid__c`",
            "string",
        ),
        (
            "vlocity_cmt__assetreferenceid__c",
            "string",
            "`(op) vlocity_cmt__assetreferenceid__c`",
            "string",
        ),
        (
            "vlocity_cmt__asyncoperation__c",
            "string",
            "`(op) vlocity_cmt__asyncoperation__c`",
            "string",
        ),
        (
            "vlocity_cmt__attributemetadatachanges__c",
            "string",
            "`(op) vlocity_cmt__attributemetadatachanges__c`",
            "string",
        ),
        (
            "vlocity_cmt__attributeselectedvalues__c",
            "string",
            "`(op) vlocity_cmt__attributeselectedvalues__c`",
            "string",
        ),
        (
            "vlocity_cmt__baseonetimecharge__c",
            "string",
            "`(op) vlocity_cmt__baseonetimecharge__c`",
            "string",
        ),
        (
            "vlocity_cmt__baseonetimetotal__c",
            "string",
            "`(op) vlocity_cmt__baseonetimetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__baserecurringcharge__c",
            "string",
            "`(op) vlocity_cmt__baserecurringcharge__c`",
            "string",
        ),
        (
            "vlocity_cmt__baserecurringtotal__c",
            "string",
            "`(op) vlocity_cmt__baserecurringtotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__billingaccountid__c",
            "string",
            "`(op) vlocity_cmt__billingaccountid__c`",
            "string",
        ),
        (
            "vlocity_cmt__catalogitemreferencedatetime__c",
            "string",
            "`(op) vlocity_cmt__catalogitemreferencedatetime__c`",
            "string",
        ),
        (
            "vlocity_cmt__connectdate__c",
            "string",
            "`(op) vlocity_cmt__connectdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__cpqcardinalitymessage__c",
            "string",
            "`(op) vlocity_cmt__cpqcardinalitymessage__c`",
            "string",
        ),
        (
            "vlocity_cmt__cpqmessagedata__c",
            "string",
            "`(op) vlocity_cmt__cpqmessagedata__c`",
            "string",
        ),
        (
            "vlocity_cmt__cpqpricingmessage__c",
            "string",
            "`(op) vlocity_cmt__cpqpricingmessage__c`",
            "string",
        ),
        (
            "vlocity_cmt__currencypaymentmode__c",
            "string",
            "`(op) vlocity_cmt__currencypaymentmode__c`",
            "string",
        ),
        (
            "vlocity_cmt__disconnectdate__c",
            "string",
            "`(op) vlocity_cmt__disconnectdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectivebaseonetimetotal__c",
            "string",
            "`(op) vlocity_cmt__effectivebaseonetimetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectivebaserecurringtotal__c",
            "string",
            "`(op) vlocity_cmt__effectivebaserecurringtotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveonetimecosttotal__c",
            "string",
            "`(op) vlocity_cmt__effectiveonetimecosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveonetimeloyaltytotal__c",
            "string",
            "`(op) vlocity_cmt__effectiveonetimeloyaltytotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveonetimetotal__c",
            "string",
            "`(op) vlocity_cmt__effectiveonetimetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectivequantity__c",
            "string",
            "`(op) vlocity_cmt__effectivequantity__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiverecurringcosttotal__c",
            "string",
            "`(op) vlocity_cmt__effectiverecurringcosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiverecurringtotal__c",
            "string",
            "`(op) vlocity_cmt__effectiverecurringtotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveusagecosttotal__c",
            "string",
            "`(op) vlocity_cmt__effectiveusagecosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveusagepricetotal__c",
            "string",
            "`(op) vlocity_cmt__effectiveusagepricetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveusagequantity__c",
            "string",
            "`(op) vlocity_cmt__effectiveusagequantity__c`",
            "string",
        ),
        (
            "vlocity_cmt__expectedcompletiondate__c",
            "string",
            "`(op) vlocity_cmt__expectedcompletiondate__c`",
            "string",
        ),
        ("vlocity_cmt__filter__c", "string", "`(op) vlocity_cmt__filter__c`", "string"),
        (
            "vlocity_cmt__financedamounttotal__c",
            "string",
            "`(op) vlocity_cmt__financedamounttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__financedamount__c",
            "string",
            "`(op) vlocity_cmt__financedamount__c`",
            "string",
        ),
        (
            "vlocity_cmt__financedinstallmentamount__c",
            "string",
            "`(op) vlocity_cmt__financedinstallmentamount__c`",
            "string",
        ),
        (
            "vlocity_cmt__firstversionorderitemid__c",
            "string",
            "`(op) vlocity_cmt__firstversionorderitemid__c`",
            "string",
        ),
        (
            "vlocity_cmt__fulfilmentstatus__c",
            "string",
            "`(op) vlocity_cmt__fulfilmentstatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__incartquantitymap__c",
            "string",
            "`(op) vlocity_cmt__incartquantitymap__c`",
            "string",
        ),
        (
            "vlocity_cmt__ischangesaccepted__c",
            "string",
            "`(op) vlocity_cmt__ischangesaccepted__c`",
            "string",
        ),
        (
            "vlocity_cmt__ischangesallowed__c",
            "string",
            "`(op) vlocity_cmt__ischangesallowed__c`",
            "string",
        ),
        (
            "vlocity_cmt__iseditable__c",
            "string",
            "`(op) vlocity_cmt__iseditable__c`",
            "string",
        ),
        (
            "vlocity_cmt__isorchestrationitemsinfinalstate__c",
            "string",
            "`(op) vlocity_cmt__isorchestrationitemsinfinalstate__c`",
            "string",
        ),
        (
            "vlocity_cmt__isponrreached__c",
            "string",
            "`(op) vlocity_cmt__isponrreached__c`",
            "string",
        ),
        (
            "vlocity_cmt__ispriced__c",
            "string",
            "`(op) vlocity_cmt__ispriced__c`",
            "string",
        ),
        (
            "vlocity_cmt__isproducttrackagreement__c",
            "string",
            "`(op) vlocity_cmt__isproducttrackagreement__c`",
            "string",
        ),
        (
            "vlocity_cmt__isreadyforactivation__c",
            "string",
            "`(op) vlocity_cmt__isreadyforactivation__c`",
            "string",
        ),
        (
            "vlocity_cmt__isvalidated__c",
            "string",
            "`(op) vlocity_cmt__isvalidated__c`",
            "string",
        ),
        (
            "vlocity_cmt__itemname__c",
            "string",
            "`(op) vlocity_cmt__itemname__c`",
            "string",
        ),
        (
            "vlocity_cmt__jsonattribute__c",
            "string",
            "`(op) vlocity_cmt__jsonattribute__c`",
            "string",
        ),
        (
            "vlocity_cmt__jsonnode__c",
            "string",
            "`(op) vlocity_cmt__jsonnode__c`",
            "string",
        ),
        (
            "vlocity_cmt__lastpricedat__c",
            "string",
            "`(op) vlocity_cmt__lastpricedat__c`",
            "string",
        ),
        (
            "vlocity_cmt__lineitemnumstring__c",
            "string",
            "`(op) vlocity_cmt__lineitemnumstring__c`",
            "string",
        ),
        (
            "vlocity_cmt__lineitemnumber__c",
            "string",
            "`(op) vlocity_cmt__lineitemnumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__linenumber__c",
            "string",
            "`(op) vlocity_cmt__linenumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__lockedby__c",
            "string",
            "`(op) vlocity_cmt__lockedby__c`",
            "string",
        ),
        (
            "vlocity_cmt__lockedfor__c",
            "string",
            "`(op) vlocity_cmt__lockedfor__c`",
            "string",
        ),
        ("vlocity_cmt__mrc__c", "string", "`(op) vlocity_cmt__mrc__c`", "string"),
        (
            "vlocity_cmt__mainorderitemid__c",
            "string",
            "`(op) vlocity_cmt__mainorderitemid__c`",
            "string",
        ),
        (
            "vlocity_cmt__modification__c",
            "string",
            "`(op) vlocity_cmt__modification__c`",
            "string",
        ),
        (
            "vlocity_cmt__monthlytotal__c",
            "string",
            "`(op) vlocity_cmt__monthlytotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimecalculatedprice__c",
            "string",
            "`(op) vlocity_cmt__onetimecalculatedprice__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimecharge__c",
            "string",
            "`(op) vlocity_cmt__onetimecharge__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimecosttotal__c",
            "string",
            "`(op) vlocity_cmt__onetimecosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimecost__c",
            "string",
            "`(op) vlocity_cmt__onetimecost__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimediscountprice__c",
            "string",
            "`(op) vlocity_cmt__onetimediscountprice__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimeloyaltyprice__c",
            "string",
            "`(op) vlocity_cmt__onetimeloyaltyprice__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimeloyaltytotal__c",
            "string",
            "`(op) vlocity_cmt__onetimeloyaltytotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimemanualdiscount__c",
            "string",
            "`(op) vlocity_cmt__onetimemanualdiscount__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimemargin__c",
            "string",
            "`(op) vlocity_cmt__onetimemargin__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimetotal__c",
            "string",
            "`(op) vlocity_cmt__onetimetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__ordergroupid__c",
            "string",
            "`(op) vlocity_cmt__ordergroupid__c`",
            "string",
        ),
        (
            "vlocity_cmt__ordermemberid__c",
            "string",
            "`(op) vlocity_cmt__ordermemberid__c`",
            "string",
        ),
        (
            "vlocity_cmt__overagecalculatedprice__c",
            "string",
            "`(op) vlocity_cmt__overagecalculatedprice__c`",
            "string",
        ),
        (
            "vlocity_cmt__overagecharge__c",
            "string",
            "`(op) vlocity_cmt__overagecharge__c`",
            "string",
        ),
        (
            "vlocity_cmt__overagediscountprice__c",
            "string",
            "`(op) vlocity_cmt__overagediscountprice__c`",
            "string",
        ),
        (
            "vlocity_cmt__overagemanualdiscount__c",
            "string",
            "`(op) vlocity_cmt__overagemanualdiscount__c`",
            "string",
        ),
        (
            "vlocity_cmt__overagetotal__c",
            "string",
            "`(op) vlocity_cmt__overagetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__overageuom__c",
            "string",
            "`(op) vlocity_cmt__overageuom__c`",
            "string",
        ),
        (
            "vlocity_cmt__parentitemid__c",
            "string",
            "`(op) vlocity_cmt__parentitemid__c`",
            "string",
        ),
        (
            "vlocity_cmt__parentlineitemnumber__c",
            "string",
            "`(op) vlocity_cmt__parentlineitemnumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__parentorderproduct__c",
            "string",
            "`(op) vlocity_cmt__parentorderproduct__c`",
            "string",
        ),
        (
            "vlocity_cmt__premisesid__c",
            "string",
            "`(op) vlocity_cmt__premisesid__c`",
            "string",
        ),
        (
            "vlocity_cmt__pricinglogdata__c",
            "string",
            "`(op) vlocity_cmt__pricinglogdata__c`",
            "string",
        ),
        (
            "vlocity_cmt__product2id__c",
            "string",
            "`(op) vlocity_cmt__product2id__c`",
            "string",
        ),
        (
            "vlocity_cmt__productattribxn__c",
            "string",
            "`(op) vlocity_cmt__productattribxn__c`",
            "string",
        ),
        (
            "vlocity_cmt__productgroupkey__c",
            "string",
            "`(op) vlocity_cmt__productgroupkey__c`",
            "string",
        ),
        (
            "vlocity_cmt__producthierarchygroupkeypath__c",
            "string",
            "`(op) vlocity_cmt__producthierarchygroupkeypath__c`",
            "string",
        ),
        (
            "vlocity_cmt__producthierarchypath__c",
            "string",
            "`(op) vlocity_cmt__producthierarchypath__c`",
            "string",
        ),
        (
            "vlocity_cmt__provisioningstatus__c",
            "string",
            "`(op) vlocity_cmt__provisioningstatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__pusheventdata__c",
            "string",
            "`(op) vlocity_cmt__pusheventdata__c`",
            "string",
        ),
        ("vlocity_cmt__query__c", "string", "`(op) vlocity_cmt__query__c`", "string"),
        (
            "vlocity_cmt__recurringcalculatedprice__c",
            "string",
            "`(op) vlocity_cmt__recurringcalculatedprice__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringcharge__c",
            "string",
            "`(op) vlocity_cmt__recurringcharge__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringcosttotal__c",
            "string",
            "`(op) vlocity_cmt__recurringcosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringcost__c",
            "string",
            "`(op) vlocity_cmt__recurringcost__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringdiscountprice__c",
            "string",
            "`(op) vlocity_cmt__recurringdiscountprice__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringmanualdiscount__c",
            "string",
            "`(op) vlocity_cmt__recurringmanualdiscount__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringmargin__c",
            "string",
            "`(op) vlocity_cmt__recurringmargin__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringtotal__c",
            "string",
            "`(op) vlocity_cmt__recurringtotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringuom__c",
            "string",
            "`(op) vlocity_cmt__recurringuom__c`",
            "string",
        ),
        (
            "vlocity_cmt__relationshiptype__c",
            "string",
            "`(op) vlocity_cmt__relationshiptype__c`",
            "string",
        ),
        (
            "vlocity_cmt__reliesonitemid__c",
            "string",
            "`(op) vlocity_cmt__reliesonitemid__c`",
            "string",
        ),
        (
            "vlocity_cmt__requestdate__c",
            "string",
            "`(op) vlocity_cmt__requestdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__requestedchange__c",
            "string",
            "`(op) vlocity_cmt__requestedchange__c`",
            "string",
        ),
        (
            "vlocity_cmt__requestedcompletiondate__c",
            "string",
            "`(op) vlocity_cmt__requestedcompletiondate__c`",
            "string",
        ),
        (
            "vlocity_cmt__requestedstartdate__c",
            "string",
            "`(op) vlocity_cmt__requestedstartdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__rootitemid__c",
            "string",
            "`(op) vlocity_cmt__rootitemid__c`",
            "string",
        ),
        (
            "vlocity_cmt__sequencenumber__c",
            "string",
            "`(op) vlocity_cmt__sequencenumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__serialnumber__c",
            "string",
            "`(op) vlocity_cmt__serialnumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__serviceaccountid__c",
            "string",
            "`(op) vlocity_cmt__serviceaccountid__c`",
            "string",
        ),
        (
            "vlocity_cmt__serviceidentifier__c",
            "string",
            "`(op) vlocity_cmt__serviceidentifier__c`",
            "string",
        ),
        (
            "vlocity_cmt__servicepointid__c",
            "string",
            "`(op) vlocity_cmt__servicepointid__c`",
            "string",
        ),
        (
            "vlocity_cmt__subaction__c",
            "string",
            "`(op) vlocity_cmt__subaction__c`",
            "string",
        ),
        (
            "vlocity_cmt__subscriptionid__c",
            "string",
            "`(op) vlocity_cmt__subscriptionid__c`",
            "string",
        ),
        (
            "vlocity_cmt__supersededorderitemid__c",
            "string",
            "`(op) vlocity_cmt__supersededorderitemid__c`",
            "string",
        ),
        (
            "vlocity_cmt__supplementalaction__c",
            "string",
            "`(op) vlocity_cmt__supplementalaction__c`",
            "string",
        ),
        (
            "vlocity_cmt__thorjeopardystatus__c",
            "string",
            "`(op) vlocity_cmt__thorjeopardystatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__usagecosttotal__c",
            "string",
            "`(op) vlocity_cmt__usagecosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__usagemargin__c",
            "string",
            "`(op) vlocity_cmt__usagemargin__c`",
            "string",
        ),
        (
            "vlocity_cmt__usagemeasurementid__c",
            "string",
            "`(op) vlocity_cmt__usagemeasurementid__c`",
            "string",
        ),
        (
            "vlocity_cmt__usagepricetotal__c",
            "string",
            "`(op) vlocity_cmt__usagepricetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__usagequantity__c",
            "string",
            "`(op) vlocity_cmt__usagequantity__c`",
            "string",
        ),
        (
            "vlocity_cmt__usageunitcost__c",
            "string",
            "`(op) vlocity_cmt__usageunitcost__c`",
            "string",
        ),
        (
            "vlocity_cmt__usageunitprice__c",
            "string",
            "`(op) vlocity_cmt__usageunitprice__c`",
            "string",
        ),
        (
            "vlocity_cmt__validationdate__c",
            "string",
            "`(op) vlocity_cmt__validationdate__c`",
            "string",
        ),
        ("swm_market_price__c", "string", "`(op) swm_market_price__c`", "string"),
        ("swm_floor_rate__c", "string", "`(op) swm_floor_rate__c`", "string"),
        ("buy_type__c", "string", "`(op) buy_type__c`", "string"),
        ("filename", "string", "`(op) filename`", "string"),
        ("function_name", "string", "`(op) function_name`", "string"),
        ("function_version", "string", "`(op) function_version`", "string"),
        ("aws_request_id", "string", "`(op) aws_request_id`", "string"),
        ("appflow_id", "string", "`(op) appflow_id`", "string"),
        ("processed_ts", "timestamp", "`(op) processed_ts`", "timestamp"),
        ("prior_oli_id__c", "string", "`(op) prior_oli_id__c`", "string"),
        ("swm_optimize__c", "string", "`(op) swm_optimize__c`", "string"),
        ("quotelineitemid", "string", "`(op) quotelineitemid`", "string"),
        ("partition_0", "string", "`(op) partition_0`", "string"),
        ("partition_1", "string", "`(op) partition_1`", "string"),
        ("partition_2", "string", "`(op) partition_2`", "string"),
        ("partition_3", "string", "`(op) partition_3`", "string"),
    ],
    transformation_ctx="RenamedOrderProduct_node1656582906626",
)

# Script generated for node OrderJoinsOrderAdPlacement
OrderJoinsOrderAdPlacement_node1656582525721 = Join.apply(
    frame1=Order_node1656582415757,
    frame2=RenamedOrderAdPlacement_node1656582564229,
    keys1=["id"],
    keys2=["`(oap) vlocity_cmt__orderid__c`"],
    transformation_ctx="OrderJoinsOrderAdPlacement_node1656582525721",
)

# Script generated for node OrderJoinsContract
OrderJoinsContract_node1656582659826 = Join.apply(
    frame1=OrderJoinsOrderAdPlacement_node1656582525721,
    frame2=RenamedContract_node1656582709130,
    keys1=["id"],
    keys2=["`(cntr) vlocity_cmt__orderid__c`"],
    transformation_ctx="OrderJoinsContract_node1656582659826",
)

# Script generated for node OrderJoinsOrderProduct
OrderJoinsOrderProduct_node1656582856053 = Join.apply(
    frame1=OrderJoinsContract_node1656582659826,
    frame2=RenamedOrderProduct_node1656582906626,
    keys1=["id"],
    keys2=["`(op) orderid`"],
    transformation_ctx="OrderJoinsOrderProduct_node1656582856053",
)

# Script generated for node Apply Mapping
ApplyMapping_node1656582957314 = ApplyMapping.apply(
    frame=OrderJoinsOrderProduct_node1656582856053,
    mappings=[
        ("`(cntr) id`", "string", "SLI_Reservation_ID", "string"),
        (
            "`(oap) vlocity_cmt__orderitemid__c`",
            "string",
            "Sales_Order_Line_Item_ID",
            "string",
        ),
        ("id", "string", "Sales_Order_ID", "string"),
        ("`(cntr) status`", "string", "Reservation_Status", "string"),
        ("`(cntr) startdate`", "string", "Requested_Time", "string"),
        ("`(cntr) enddate`", "string", "Committed_Time", "string"),
        (
            "`(cntr) vlocity_cmt__terminatedate__c`",
            "string",
            "Expiration_Date",
            "string",
        ),
        ("`(op) availablequantity`", "string", "Reserved_Quantity", "string"),
    ],
    transformation_ctx="ApplyMapping_node1656582957314",
)

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = ApplyMapping_node1656582957314.toDF().coalesce(1)

# Added columns not available in Saleforce
partitioned_dataframe = partitioned_dataframe \
            .withColumn("Contracted_Producton_Quantity",lit(None)) \
            .withColumn("OP1_SLI_Reservation_ID",lit(None)) \
            .withColumn("OP1_Sales_Order_Line_Item_ID",lit(None)) \
            .withColumn("OP1_Sales_Order_ID",lit(None))

# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

# Script generated for node Amazon S3
Synth_Sales_Order_Line_Items_Reservation = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://swmi-etldev-sf-to-dw-dwmock",
        "partitionKeys": [],
    },
    transformation_ctx="Synth_Sales_Order_Line_Items_Reservation",
)

job.commit()

###############################################################
## Clowning the existing file and deleting the old file.    ###
###############################################################

bucket_name = "swmi-etldev-sf-to-dw-dwmock"
filename = "Synth-Sales_Order_Line_Items_Reservation" + "_" + str(datetime.now().strftime("%Y-%m-%d %H%M%S"))

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
