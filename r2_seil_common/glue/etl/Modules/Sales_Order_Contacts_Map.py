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
Order_node1655395816592 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_order",
    transformation_ctx="Order_node1655395816592",
)

# Script generated for node Contact
Contact_node1655395860292 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_contact",
    transformation_ctx="Contact_node1655395860292",
)

# Script generated for node Account
Account_node1655395908346 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_account",
    transformation_ctx="Account_node1655395908346",
)

# Script generated for node RenamedOrder
RenamedOrder_node1655396116039 = ApplyMapping.apply(
    frame=Order_node1655395816592,
    mappings=[
        ("id", "string", "`(o) id`", "string"),
        ("ownerid", "string", "`(o) ownerid`", "string"),
        ("contractid", "string", "`(o) contractid`", "string"),
        ("accountid", "string", "`(o) accountid`", "string"),
        ("pricebook2id", "string", "`(o) pricebook2id`", "string"),
        ("originalorderid", "string", "`(o) originalorderid`", "string"),
        ("opportunityid", "string", "`(o) opportunityid`", "string"),
        ("quoteid", "string", "`(o) quoteid`", "string"),
        ("recordtypeid", "string", "`(o) recordtypeid`", "string"),
        ("effectivedate", "string", "`(o) effectivedate`", "string"),
        ("enddate", "string", "`(o) enddate`", "string"),
        ("isreductionorder", "string", "`(o) isreductionorder`", "string"),
        ("status", "string", "`(o) status`", "string"),
        ("description", "string", "`(o) description`", "string"),
        ("customerauthorizedbyid", "string", "`(o) customerauthorizedbyid`", "string"),
        ("companyauthorizedbyid", "string", "`(o) companyauthorizedbyid`", "string"),
        ("type", "string", "`(o) type`", "string"),
        ("billingstreet", "string", "`(o) billingstreet`", "string"),
        ("billingcity", "string", "`(o) billingcity`", "string"),
        ("billingstate", "string", "`(o) billingstate`", "string"),
        ("billingpostalcode", "string", "`(o) billingpostalcode`", "string"),
        ("billingcountry", "string", "`(o) billingcountry`", "string"),
        ("billinglatitude", "string", "`(o) billinglatitude`", "string"),
        ("billinglongitude", "string", "`(o) billinglongitude`", "string"),
        ("billinggeocodeaccuracy", "string", "`(o) billinggeocodeaccuracy`", "string"),
        ("billingaddress", "string", "`(o) billingaddress`", "string"),
        ("shippingstreet", "string", "`(o) shippingstreet`", "string"),
        ("shippingcity", "string", "`(o) shippingcity`", "string"),
        ("shippingstate", "string", "`(o) shippingstate`", "string"),
        ("shippingpostalcode", "string", "`(o) shippingpostalcode`", "string"),
        ("shippingcountry", "string", "`(o) shippingcountry`", "string"),
        ("shippinglatitude", "string", "`(o) shippinglatitude`", "string"),
        ("shippinglongitude", "string", "`(o) shippinglongitude`", "string"),
        (
            "shippinggeocodeaccuracy",
            "string",
            "`(o) shippinggeocodeaccuracy`",
            "string",
        ),
        ("shippingaddress", "string", "`(o) shippingaddress`", "string"),
        ("activateddate", "string", "`(o) activateddate`", "string"),
        ("activatedbyid", "string", "`(o) activatedbyid`", "string"),
        ("statuscode", "string", "`(o) statuscode`", "string"),
        ("ordernumber", "string", "`(o) ordernumber`", "string"),
        ("totalamount", "string", "`(o) totalamount`", "string"),
        ("createddate", "string", "`(o) createddate`", "string"),
        ("createdbyid", "string", "`(o) createdbyid`", "string"),
        ("lastmodifieddate", "string", "`(o) lastmodifieddate`", "string"),
        ("lastmodifiedbyid", "string", "`(o) lastmodifiedbyid`", "string"),
        ("isdeleted", "string", "`(o) isdeleted`", "string"),
        ("systemmodstamp", "string", "`(o) systemmodstamp`", "string"),
        ("lastvieweddate", "string", "`(o) lastvieweddate`", "string"),
        ("lastreferenceddate", "string", "`(o) lastreferenceddate`", "string"),
        (
            "vlocity_cmt__accountid__c",
            "string",
            "`(o) vlocity_cmt__accountid__c`",
            "string",
        ),
        (
            "vlocity_cmt__accountrecordtype__c",
            "string",
            "`(o) vlocity_cmt__accountrecordtype__c`",
            "string",
        ),
        (
            "vlocity_cmt__accountsla__c",
            "string",
            "`(o) vlocity_cmt__accountsla__c`",
            "string",
        ),
        (
            "vlocity_cmt__asyncjobid__c",
            "string",
            "`(o) vlocity_cmt__asyncjobid__c`",
            "string",
        ),
        (
            "vlocity_cmt__asyncoperation__c",
            "string",
            "`(o) vlocity_cmt__asyncoperation__c`",
            "string",
        ),
        (
            "vlocity_cmt__billingname__c",
            "string",
            "`(o) vlocity_cmt__billingname__c`",
            "string",
        ),
        (
            "vlocity_cmt__campaignid__c",
            "string",
            "`(o) vlocity_cmt__campaignid__c`",
            "string",
        ),
        (
            "vlocity_cmt__cartidentifier__c",
            "string",
            "`(o) vlocity_cmt__cartidentifier__c`",
            "string",
        ),
        (
            "vlocity_cmt__createdbyapi__c",
            "string",
            "`(o) vlocity_cmt__createdbyapi__c`",
            "string",
        ),
        (
            "vlocity_cmt__creditcheckdecision__c",
            "string",
            "`(o) vlocity_cmt__creditcheckdecision__c`",
            "string",
        ),
        (
            "vlocity_cmt__customeroriginatedbyid__c",
            "string",
            "`(o) vlocity_cmt__customeroriginatedbyid__c`",
            "string",
        ),
        (
            "vlocity_cmt__defaultbillingaccountid__c",
            "string",
            "`(o) vlocity_cmt__defaultbillingaccountid__c`",
            "string",
        ),
        (
            "vlocity_cmt__defaultcurrencypaymentmode__c",
            "string",
            "`(o) vlocity_cmt__defaultcurrencypaymentmode__c`",
            "string",
        ),
        (
            "vlocity_cmt__defaultpremisesid__c",
            "string",
            "`(o) vlocity_cmt__defaultpremisesid__c`",
            "string",
        ),
        (
            "vlocity_cmt__defaultserviceaccountid__c",
            "string",
            "`(o) vlocity_cmt__defaultserviceaccountid__c`",
            "string",
        ),
        (
            "vlocity_cmt__defaultservicepointid__c",
            "string",
            "`(o) vlocity_cmt__defaultservicepointid__c`",
            "string",
        ),
        (
            "vlocity_cmt__deliverymethod__c",
            "string",
            "`(o) vlocity_cmt__deliverymethod__c`",
            "string",
        ),
        (
            "vlocity_cmt__delivery_installation_status__c",
            "string",
            "`(o) vlocity_cmt__delivery_installation_status__c`",
            "string",
        ),
        (
            "vlocity_cmt__discount__c",
            "string",
            "`(o) vlocity_cmt__discount__c`",
            "string",
        ),
        (
            "vlocity_cmt__duedate__c",
            "string",
            "`(o) vlocity_cmt__duedate__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectivebaseonetimecharge__c",
            "string",
            "`(o) vlocity_cmt__effectivebaseonetimecharge__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectivebaserecurringcharge__c",
            "string",
            "`(o) vlocity_cmt__effectivebaserecurringcharge__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveonetimecosttotal__c",
            "string",
            "`(o) vlocity_cmt__effectiveonetimecosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveonetimeloyaltytotal__c",
            "string",
            "`(o) vlocity_cmt__effectiveonetimeloyaltytotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveordertotal__c",
            "string",
            "`(o) vlocity_cmt__effectiveordertotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiverecurringcosttotal__c",
            "string",
            "`(o) vlocity_cmt__effectiverecurringcosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveusagecosttotal__c",
            "string",
            "`(o) vlocity_cmt__effectiveusagecosttotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveusagepricetotal__c",
            "string",
            "`(o) vlocity_cmt__effectiveusagepricetotal__c`",
            "string",
        ),
        ("vlocity_cmt__email__c", "string", "`(o) vlocity_cmt__email__c`", "string"),
        (
            "vlocity_cmt__expirationdate__c",
            "string",
            "`(o) vlocity_cmt__expirationdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__externalpricingstatus__c",
            "string",
            "`(o) vlocity_cmt__externalpricingstatus__c`",
            "string",
        ),
        ("vlocity_cmt__fax__c", "string", "`(o) vlocity_cmt__fax__c`", "string"),
        (
            "vlocity_cmt__firstsubmitattemptts__c",
            "string",
            "`(o) vlocity_cmt__firstsubmitattemptts__c`",
            "string",
        ),
        (
            "vlocity_cmt__firstversionorderidentifier__c",
            "string",
            "`(o) vlocity_cmt__firstversionorderidentifier__c`",
            "string",
        ),
        (
            "vlocity_cmt__framecontractid__c",
            "string",
            "`(o) vlocity_cmt__framecontractid__c`",
            "string",
        ),
        (
            "vlocity_cmt__fulfilmentstatus__c",
            "string",
            "`(o) vlocity_cmt__fulfilmentstatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__isactiveorderversion__c",
            "string",
            "`(o) vlocity_cmt__isactiveorderversion__c`",
            "string",
        ),
        (
            "vlocity_cmt__ischangesaccepted__c",
            "string",
            "`(o) vlocity_cmt__ischangesaccepted__c`",
            "string",
        ),
        (
            "vlocity_cmt__ischangesallowed__c",
            "string",
            "`(o) vlocity_cmt__ischangesallowed__c`",
            "string",
        ),
        (
            "vlocity_cmt__iscontractrequired__c",
            "string",
            "`(o) vlocity_cmt__iscontractrequired__c`",
            "string",
        ),
        (
            "vlocity_cmt__ispriced__c",
            "string",
            "`(o) vlocity_cmt__ispriced__c`",
            "string",
        ),
        (
            "vlocity_cmt__issyncing__c",
            "string",
            "`(o) vlocity_cmt__issyncing__c`",
            "string",
        ),
        (
            "vlocity_cmt__isvalidated__c",
            "string",
            "`(o) vlocity_cmt__isvalidated__c`",
            "string",
        ),
        (
            "vlocity_cmt__jeopardysafetyintervalunit__c",
            "string",
            "`(o) vlocity_cmt__jeopardysafetyintervalunit__c`",
            "string",
        ),
        (
            "vlocity_cmt__jeopardysafetyinterval__c",
            "string",
            "`(o) vlocity_cmt__jeopardysafetyinterval__c`",
            "string",
        ),
        (
            "vlocity_cmt__jeopardystatus__c",
            "string",
            "`(o) vlocity_cmt__jeopardystatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__lastpricedat__c",
            "string",
            "`(o) vlocity_cmt__lastpricedat__c`",
            "string",
        ),
        (
            "vlocity_cmt__leadsource__c",
            "string",
            "`(o) vlocity_cmt__leadsource__c`",
            "string",
        ),
        (
            "vlocity_cmt__lockedby__c",
            "string",
            "`(o) vlocity_cmt__lockedby__c`",
            "string",
        ),
        (
            "vlocity_cmt__lockedfor__c",
            "string",
            "`(o) vlocity_cmt__lockedfor__c`",
            "string",
        ),
        (
            "vlocity_cmt__masterordername__c",
            "string",
            "`(o) vlocity_cmt__masterordername__c`",
            "string",
        ),
        (
            "vlocity_cmt__maxcataloguedeltaobjectindex__c",
            "string",
            "`(o) vlocity_cmt__maxcataloguedeltaobjectindex__c`",
            "string",
        ),
        (
            "vlocity_cmt__multiservicemessagedata__c",
            "string",
            "`(o) vlocity_cmt__multiservicemessagedata__c`",
            "string",
        ),
        ("vlocity_cmt__notes__c", "string", "`(o) vlocity_cmt__notes__c`", "string"),
        (
            "vlocity_cmt__numberofcontractedmonths__c",
            "string",
            "`(o) vlocity_cmt__numberofcontractedmonths__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimeloyaltytotal__c",
            "string",
            "`(o) vlocity_cmt__onetimeloyaltytotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimemargintotal__c",
            "string",
            "`(o) vlocity_cmt__onetimemargintotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimetotal2__c",
            "string",
            "`(o) vlocity_cmt__onetimetotal2__c`",
            "string",
        ),
        (
            "vlocity_cmt__opportunityid__c",
            "string",
            "`(o) vlocity_cmt__opportunityid__c`",
            "string",
        ),
        (
            "vlocity_cmt__orchestrationplanid__c",
            "string",
            "`(o) vlocity_cmt__orchestrationplanid__c`",
            "string",
        ),
        (
            "vlocity_cmt__orchestrationplanreferenceid__c",
            "string",
            "`(o) vlocity_cmt__orchestrationplanreferenceid__c`",
            "string",
        ),
        (
            "vlocity_cmt__ordergroupid__c",
            "string",
            "`(o) vlocity_cmt__ordergroupid__c`",
            "string",
        ),
        (
            "vlocity_cmt__ordergroup__c",
            "string",
            "`(o) vlocity_cmt__ordergroup__c`",
            "string",
        ),
        (
            "vlocity_cmt__ordermargintotal__c",
            "string",
            "`(o) vlocity_cmt__ordermargintotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__orderstatus__c",
            "string",
            "`(o) vlocity_cmt__orderstatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__ordertotal__c",
            "string",
            "`(o) vlocity_cmt__ordertotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__originatingchannel__c",
            "string",
            "`(o) vlocity_cmt__originatingchannel__c`",
            "string",
        ),
        (
            "vlocity_cmt__originatingcontactid__c",
            "string",
            "`(o) vlocity_cmt__originatingcontactid__c`",
            "string",
        ),
        (
            "vlocity_cmt__originatingcontractid__c",
            "string",
            "`(o) vlocity_cmt__originatingcontractid__c`",
            "string",
        ),
        (
            "vlocity_cmt__parentorderid__c",
            "string",
            "`(o) vlocity_cmt__parentorderid__c`",
            "string",
        ),
        ("vlocity_cmt__phone__c", "string", "`(o) vlocity_cmt__phone__c`", "string"),
        (
            "vlocity_cmt__pricelistid__c",
            "string",
            "`(o) vlocity_cmt__pricelistid__c`",
            "string",
        ),
        (
            "vlocity_cmt__pricebook__c",
            "string",
            "`(o) vlocity_cmt__pricebook__c`",
            "string",
        ),
        (
            "vlocity_cmt__processinglog__c",
            "string",
            "`(o) vlocity_cmt__processinglog__c`",
            "string",
        ),
        (
            "vlocity_cmt__queuedts__c",
            "string",
            "`(o) vlocity_cmt__queuedts__c`",
            "string",
        ),
        (
            "vlocity_cmt__quoteid__c",
            "string",
            "`(o) vlocity_cmt__quoteid__c`",
            "string",
        ),
        (
            "vlocity_cmt__quotetocity__c",
            "string",
            "`(o) vlocity_cmt__quotetocity__c`",
            "string",
        ),
        (
            "vlocity_cmt__quotetocountry__c",
            "string",
            "`(o) vlocity_cmt__quotetocountry__c`",
            "string",
        ),
        (
            "vlocity_cmt__quotetoname__c",
            "string",
            "`(o) vlocity_cmt__quotetoname__c`",
            "string",
        ),
        (
            "vlocity_cmt__quotetopostalcode__c",
            "string",
            "`(o) vlocity_cmt__quotetopostalcode__c`",
            "string",
        ),
        (
            "vlocity_cmt__quotetostate__c",
            "string",
            "`(o) vlocity_cmt__quotetostate__c`",
            "string",
        ),
        (
            "vlocity_cmt__quotetostreet__c",
            "string",
            "`(o) vlocity_cmt__quotetostreet__c`",
            "string",
        ),
        ("vlocity_cmt__reason__c", "string", "`(o) vlocity_cmt__reason__c`", "string"),
        (
            "vlocity_cmt__recurringmargintotal__c",
            "string",
            "`(o) vlocity_cmt__recurringmargintotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringtotal2__c",
            "string",
            "`(o) vlocity_cmt__recurringtotal2__c`",
            "string",
        ),
        (
            "vlocity_cmt__requestdate__c",
            "string",
            "`(o) vlocity_cmt__requestdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__requestedcompletiondate__c",
            "string",
            "`(o) vlocity_cmt__requestedcompletiondate__c`",
            "string",
        ),
        (
            "vlocity_cmt__requestedstartdate__c",
            "string",
            "`(o) vlocity_cmt__requestedstartdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__shippingname__c",
            "string",
            "`(o) vlocity_cmt__shippingname__c`",
            "string",
        ),
        (
            "vlocity_cmt__shippingpostalcode__c",
            "string",
            "`(o) vlocity_cmt__shippingpostalcode__c`",
            "string",
        ),
        (
            "vlocity_cmt__shippingstate__c",
            "string",
            "`(o) vlocity_cmt__shippingstate__c`",
            "string",
        ),
        (
            "vlocity_cmt__statusimagename__c",
            "string",
            "`(o) vlocity_cmt__statusimagename__c`",
            "string",
        ),
        (
            "vlocity_cmt__submittedtoomdate__c",
            "string",
            "`(o) vlocity_cmt__submittedtoomdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__supersededorderid__c",
            "string",
            "`(o) vlocity_cmt__supersededorderid__c`",
            "string",
        ),
        (
            "vlocity_cmt__supplementalaction__c",
            "string",
            "`(o) vlocity_cmt__supplementalaction__c`",
            "string",
        ),
        ("vlocity_cmt__tax__c", "string", "`(o) vlocity_cmt__tax__c`", "string"),
        (
            "vlocity_cmt__thorjeopardystatus__c",
            "string",
            "`(o) vlocity_cmt__thorjeopardystatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__trackingnumber__c",
            "string",
            "`(o) vlocity_cmt__trackingnumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__usagemargintotal__c",
            "string",
            "`(o) vlocity_cmt__usagemargintotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__validationdate__c",
            "string",
            "`(o) vlocity_cmt__validationdate__c`",
            "string",
        ),
        (
            "vlocity_cmt__validationmessage__c",
            "string",
            "`(o) vlocity_cmt__validationmessage__c`",
            "string",
        ),
        (
            "vlocity_cmt__validationstatus__c",
            "string",
            "`(o) vlocity_cmt__validationstatus__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiveonetimetotal__c",
            "string",
            "`(o) vlocity_cmt__effectiveonetimetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__effectiverecurringtotal__c",
            "string",
            "`(o) vlocity_cmt__effectiverecurringtotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__onetimetotal__c",
            "string",
            "`(o) vlocity_cmt__onetimetotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__recurringtotal__c",
            "string",
            "`(o) vlocity_cmt__recurringtotal__c`",
            "string",
        ),
        (
            "vlocity_cmt__totalmonthlydiscount__c",
            "string",
            "`(o) vlocity_cmt__totalmonthlydiscount__c`",
            "string",
        ),
        (
            "vlocity_cmt__totalonetimediscount__c",
            "string",
            "`(o) vlocity_cmt__totalonetimediscount__c`",
            "string",
        ),
        (
            "share_with_seven_adops__c",
            "string",
            "`(o) share_with_seven_adops__c`",
            "string",
        ),
        (
            "swm_market_rate_total__c",
            "string",
            "`(o) swm_market_rate_total__c`",
            "string",
        ),
        (
            "swm_floor_rate_total__c",
            "string",
            "`(o) swm_floor_rate_total__c`",
            "string",
        ),
        ("swm_one_time_total__c", "string", "`(o) swm_one_time_total__c`", "string"),
        ("invoice_batch_no__c", "string", "`(o) invoice_batch_no__c`", "string"),
        ("next_billing_date__c", "string", "`(o) next_billing_date__c`", "string"),
        ("swm_gam_lines__c", "string", "`(o) swm_gam_lines__c`", "string"),
        ("swm_non_gam_lines__c", "string", "`(o) swm_non_gam_lines__c`", "string"),
        (
            "swm_contains_gam_lines__c",
            "string",
            "`(o) swm_contains_gam_lines__c`",
            "string",
        ),
        (
            "swm_contains_non_gam_lines__c",
            "string",
            "`(o) swm_contains_non_gam_lines__c`",
            "string",
        ),
        ("filename", "string", "`(o) filename`", "string"),
        ("function_name", "string", "`(o) function_name`", "string"),
        ("function_version", "string", "`(o) function_version`", "string"),
        ("aws_request_id", "string", "`(o) aws_request_id`", "string"),
        ("appflow_id", "string", "`(o) appflow_id`", "string"),
        ("processed_ts", "timestamp", "`(o) processed_ts`", "timestamp"),
        ("name", "string", "`(o) name`", "string"),
        ("cm_email_id__c", "string", "`(o) cm_email_id__c`", "string"),
        ("partition_0", "string", "`(o) partition_0`", "string"),
        ("partition_1", "string", "`(o) partition_1`", "string"),
        ("partition_2", "string", "`(o) partition_2`", "string"),
        ("partition_3", "string", "`(o) partition_3`", "string"),
    ],
    transformation_ctx="RenamedOrder_node1655396116039",
)

# Script generated for node RenamedContact
RenamedContact_node1655395998883 = ApplyMapping.apply(
    frame=Contact_node1655395860292,
    mappings=[
        ("id", "string", "`(c) id`", "string"),
        ("isdeleted", "string", "`(c) isdeleted`", "string"),
        ("masterrecordid", "string", "`(c) masterrecordid`", "string"),
        ("accountid", "string", "`(c) accountid`", "string"),
        ("lastname", "string", "`(c) lastname`", "string"),
        ("firstname", "string", "`(c) firstname`", "string"),
        ("salutation", "string", "`(c) salutation`", "string"),
        ("middlename", "string", "`(c) middlename`", "string"),
        ("suffix", "string", "`(c) suffix`", "string"),
        ("name", "string", "`(c) name`", "string"),
        ("recordtypeid", "string", "`(c) recordtypeid`", "string"),
        ("mailingstreet", "string", "`(c) mailingstreet`", "string"),
        ("mailingcity", "string", "`(c) mailingcity`", "string"),
        ("mailingstate", "string", "`(c) mailingstate`", "string"),
        ("mailingpostalcode", "string", "`(c) mailingpostalcode`", "string"),
        ("mailingcountry", "string", "`(c) mailingcountry`", "string"),
        ("mailinglatitude", "string", "`(c) mailinglatitude`", "string"),
        ("mailinglongitude", "string", "`(c) mailinglongitude`", "string"),
        ("mailinggeocodeaccuracy", "string", "`(c) mailinggeocodeaccuracy`", "string"),
        ("mailingaddress", "string", "`(c) mailingaddress`", "string"),
        ("phone", "string", "`(c) phone`", "string"),
        ("fax", "string", "`(c) fax`", "string"),
        ("mobilephone", "string", "`(c) mobilephone`", "string"),
        ("reportstoid", "string", "`(c) reportstoid`", "string"),
        ("email", "string", "`(c) email`", "string"),
        ("title", "string", "`(c) title`", "string"),
        ("department", "string", "`(c) department`", "string"),
        ("ownerid", "string", "`(c) ownerid`", "string"),
        ("createddate", "string", "`(c) createddate`", "string"),
        ("createdbyid", "string", "`(c) createdbyid`", "string"),
        ("lastmodifieddate", "string", "`(c) lastmodifieddate`", "string"),
        ("lastmodifiedbyid", "string", "`(c) lastmodifiedbyid`", "string"),
        ("systemmodstamp", "string", "`(c) systemmodstamp`", "string"),
        ("lastactivitydate", "string", "`(c) lastactivitydate`", "string"),
        ("lastcurequestdate", "string", "`(c) lastcurequestdate`", "string"),
        ("lastcuupdatedate", "string", "`(c) lastcuupdatedate`", "string"),
        ("lastvieweddate", "string", "`(c) lastvieweddate`", "string"),
        ("lastreferenceddate", "string", "`(c) lastreferenceddate`", "string"),
        ("emailbouncedreason", "string", "`(c) emailbouncedreason`", "string"),
        ("emailbounceddate", "string", "`(c) emailbounceddate`", "string"),
        ("isemailbounced", "string", "`(c) isemailbounced`", "string"),
        ("photourl", "string", "`(c) photourl`", "string"),
        ("jigsaw", "string", "`(c) jigsaw`", "string"),
        ("jigsawcontactid", "string", "`(c) jigsawcontactid`", "string"),
        ("individualid", "string", "`(c) individualid`", "string"),
        ("vlocity_cmt__age__c", "string", "`(c) vlocity_cmt__age__c`", "string"),
        (
            "vlocity_cmt__annualincome__c",
            "string",
            "`(c) vlocity_cmt__annualincome__c`",
            "string",
        ),
        (
            "vlocity_cmt__authorized__c",
            "string",
            "`(c) vlocity_cmt__authorized__c`",
            "string",
        ),
        (
            "vlocity_cmt__contactnumber__c",
            "string",
            "`(c) vlocity_cmt__contactnumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__customersentiment__c",
            "string",
            "`(c) vlocity_cmt__customersentiment__c`",
            "string",
        ),
        (
            "vlocity_cmt__dayssincelastcontact__c",
            "string",
            "`(c) vlocity_cmt__dayssincelastcontact__c`",
            "string",
        ),
        (
            "vlocity_cmt__driverslicensenumber__c",
            "string",
            "`(c) vlocity_cmt__driverslicensenumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__employmentstatus__c",
            "string",
            "`(c) vlocity_cmt__employmentstatus__c`",
            "string",
        ),
        ("vlocity_cmt__gender__c", "string", "`(c) vlocity_cmt__gender__c`", "string"),
        (
            "vlocity_cmt__hasfraud__c",
            "string",
            "`(c) vlocity_cmt__hasfraud__c`",
            "string",
        ),
        ("vlocity_cmt__image__c", "string", "`(c) vlocity_cmt__image__c`", "string"),
        (
            "vlocity_cmt__isactive__c",
            "string",
            "`(c) vlocity_cmt__isactive__c`",
            "string",
        ),
        (
            "vlocity_cmt__isemployee__c",
            "string",
            "`(c) vlocity_cmt__isemployee__c`",
            "string",
        ),
        (
            "vlocity_cmt__ispartner__c",
            "string",
            "`(c) vlocity_cmt__ispartner__c`",
            "string",
        ),
        (
            "vlocity_cmt__ispersonaccount__c",
            "string",
            "`(c) vlocity_cmt__ispersonaccount__c`",
            "string",
        ),
        (
            "vlocity_cmt__lastcontactbyrecordowner__c",
            "string",
            "`(c) vlocity_cmt__lastcontactbyrecordowner__c`",
            "string",
        ),
        (
            "vlocity_cmt__location__c",
            "string",
            "`(c) vlocity_cmt__location__c`",
            "string",
        ),
        (
            "vlocity_cmt__middlename__c",
            "string",
            "`(c) vlocity_cmt__middlename__c`",
            "string",
        ),
        (
            "vlocity_cmt__networth__c",
            "string",
            "`(c) vlocity_cmt__networth__c`",
            "string",
        ),
        (
            "vlocity_cmt__occupation__c",
            "string",
            "`(c) vlocity_cmt__occupation__c`",
            "string",
        ),
        (
            "vlocity_cmt__partyid__c",
            "string",
            "`(c) vlocity_cmt__partyid__c`",
            "string",
        ),
        (
            "vlocity_cmt__primaryemployerid__c",
            "string",
            "`(c) vlocity_cmt__primaryemployerid__c`",
            "string",
        ),
        ("vlocity_cmt__ssn__c", "string", "`(c) vlocity_cmt__ssn__c`", "string"),
        (
            "vlocity_cmt__socialsecuritynumber__c",
            "string",
            "`(c) vlocity_cmt__socialsecuritynumber__c`",
            "string",
        ),
        (
            "vlocity_cmt__stateofissuance__c",
            "string",
            "`(c) vlocity_cmt__stateofissuance__c`",
            "string",
        ),
        ("vlocity_cmt__status__c", "string", "`(c) vlocity_cmt__status__c`", "string"),
        ("vlocity_cmt__taxid__c", "string", "`(c) vlocity_cmt__taxid__c`", "string"),
        ("vlocity_cmt__type__c", "string", "`(c) vlocity_cmt__type__c`", "string"),
        (
            "vlocity_cmt__useremployeenumber__c",
            "string",
            "`(c) vlocity_cmt__useremployeenumber__c`",
            "string",
        ),
        ("vlocity_cmt__userid__c", "string", "`(c) vlocity_cmt__userid__c`", "string"),
        (
            "vlocity_cmt__userleaseexpires__c",
            "string",
            "`(c) vlocity_cmt__userleaseexpires__c`",
            "string",
        ),
        (
            "vlocity_cmt__userleasetoken__c",
            "string",
            "`(c) vlocity_cmt__userleasetoken__c`",
            "string",
        ),
        (
            "vlocity_cmt__username__c",
            "string",
            "`(c) vlocity_cmt__username__c`",
            "string",
        ),
        (
            "vlocity_cmt__userpasssalt__c",
            "string",
            "`(c) vlocity_cmt__userpasssalt__c`",
            "string",
        ),
        (
            "vlocity_cmt__userpass__c",
            "string",
            "`(c) vlocity_cmt__userpass__c`",
            "string",
        ),
        (
            "vlocity_cmt__website__c",
            "string",
            "`(c) vlocity_cmt__website__c`",
            "string",
        ),
        ("filename", "string", "`(c) filename`", "string"),
        ("function_name", "string", "`(c) function_name`", "string"),
        ("function_version", "string", "`(c) function_version`", "string"),
        ("aws_request_id", "string", "`(c) aws_request_id`", "string"),
        ("appflow_id", "string", "`(c) appflow_id`", "string"),
        ("processed_ts", "timestamp", "`(c) processed_ts`", "timestamp"),
        ("partition_0", "string", "`(c) partition_0`", "string"),
        ("partition_1", "string", "`(c) partition_1`", "string"),
        ("partition_2", "string", "`(c) partition_2`", "string"),
        ("partition_3", "string", "`(c) partition_3`", "string"),
    ],
    transformation_ctx="RenamedContact_node1655395998883",
)

# Script generated for node AccountContact_Map
AccountContact_Map_node1655395942138 = Join.apply(
    frame1=Account_node1655395908346,
    frame2=RenamedContact_node1655395998883,
    keys1=["id"],
    keys2=["`(c) accountid`"],
    transformation_ctx="AccountContact_Map_node1655395942138",
)

# Script generated for node Join
Join_node1655396079399 = Join.apply(
    frame1=AccountContact_Map_node1655395942138,
    frame2=RenamedOrder_node1655396116039,
    keys1=["id"],
    keys2=["`(o) accountid`"],
    transformation_ctx="Join_node1655396079399",
)

# Script generated for node Apply Mapping
ApplyMapping_node1655396192176 = ApplyMapping.apply(
    frame=Join_node1655396079399,
    mappings=[
        ("id", "string", "Sales_Order_ID", "string"),
        (
            "vlocity_cmt__masterordername__c",
            "string",
            "Sales_Order_Name",
            "string",
        ),
        ## 3
        ("`(c) id`", "string", "Contact_ID","string"),
        ("`(c) name`", "string","Contact_Name","string"),
        ("`(c) email`", "string", "Contact_Email", "string"),
        #("`(a)SWM_Planning_Agency__c`","string", "Functional_Role_Name","string"),
        #("`(a)SWM_Planning_Agency__c`","string","Functional_Role_Internal_Name","string")
        ],
    transformation_ctx="ApplyMapping_node1655396192176",
)

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = ApplyMapping_node1655396192176.toDF().coalesce(1)

# Added columns not available in Saleforce
partitioned_dataframe = partitioned_dataframe \
    .withColumn("Functional_Role_Name",lit(None)) \
    .withColumn("Functional_Role_Internal_Name",lit(None)) \
    .withColumn("Functional_Role_ID",lit(None)) \
    .withColumn("OP1_Sales_Order_ID",lit(None)) \
    .withColumn("OP1_Contact_ID",lit(None)) \
    .withColumn("OP1_Functional_Role_ID",lit(None))

# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

# Script generated for node Amazon S3
Synth_Sales_Order_Contacts_Map = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://swmi-etldev-sf-to-dw-dwmock",
        "partitionKeys": [],
    },
    transformation_ctx="Synth_Sales_Order_Contacts_Map",
)

job.commit()

###############################################################
## Clowning the existing file and deleting the old file.    ###
###############################################################

bucket_name = "swmi-etldev-sf-to-dw-dwmock"
filename = "Synth-Sales_Order_Contacts_Map" + "_" + str(datetime.now().strftime("%Y-%m-%d %H%M%S"))

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
