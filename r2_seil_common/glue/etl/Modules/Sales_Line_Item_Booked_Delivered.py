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

# Script generated for node OrderAdPlacement
OrderAdPlacement_node1656636060337 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_order_ad_placement",
    transformation_ctx="OrderAdPlacement_node1656636060337",
)

# Script generated for node OrderProduct
OrderProduct_node1656636100653 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_order_product",
    transformation_ctx="OrderProduct_node1656636100653",
)

# Script generated for node RenamedOrderProduct
RenamedOrderProduct_node1656636192670 = ApplyMapping.apply(
    frame=OrderProduct_node1656636100653,
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
    transformation_ctx="RenamedOrderProduct_node1656636192670",
)

# Script generated for node Join
Join_node1656636169168 = Join.apply(
    frame1=OrderAdPlacement_node1656636060337,
    frame2=RenamedOrderProduct_node1656636192670,
    keys1=["vlocity_cmt__orderitemid__c"],
    keys2=["`(op) id`"],
    transformation_ctx="Join_node1656636169168",
)

# Script generated for node Apply Mapping
ApplyMapping_node1656636340275 = ApplyMapping.apply(
    frame=Join_node1656636169168,
    mappings=[
        (
            "vlocity_cmt__orderitemid__c",
            "string",
            "Sales_Order_Line_Item_ID",
            "string",
        ),
        (
            "`(op) vlocity_cmt__itemname__c`",
            "string",
            "Sales_Order_Line_Item_Name",
            "string",
        ),
        (
            "`(op) vlocity_cmt__action__c`",
            "string",
            "Booked_Actions",
            "string",
        ),
    ],
    transformation_ctx="ApplyMapping_node1656636340275",
)

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = ApplyMapping_node1656636340275.toDF().coalesce(1)

# Added columns not available in Saleforce
partitioned_dataframe = partitioned_dataframe \
            .withColumn("Production_Line_Item_ID",lit(None).cast('string')) \
            .withColumn("Production_Line_Item_Name",lit(None).cast('string')) \
            .withColumn("Delivery_Date",lit(None).cast('string')) \
            .withColumn("Booked_Impressions",lit(None).cast('string')) \
            .withColumn("Booked_Clicks",lit(None).cast('string')) \
            .withColumn("Booked_Actions",lit(None).cast('string')) \
            .withColumn("Delivered_Impressions",lit(None).cast('string')) \
            .withColumn("Delivered_Clicks",lit(None).cast('string')) \
            .withColumn("Delivered_Actions",lit(None).cast('string'))

# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

# Script generated for node Amazon S3
Synth_Sales_Line_Item_Booked_Delivered = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://swmi-etldev-sf-to-dw-dwmock",
        "partitionKeys": [],
    },
    transformation_ctx="Synth_Sales_Line_Item_Booked_Delivered",
)

job.commit()

###############################################################
## Clowning the existing file and deleting the old file.    ###
###############################################################

bucket_name = "swmi-etldev-sf-to-dw-dwmock"
filename = "Synth-Sales_Line_Item_Booked_Delivered" + "_" + str(datetime.now().strftime("%Y-%m-%d %H%M%S"))

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
