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
from pyspark.sql.functions import col, when

def convertToEmptyString(dfa):
    for i in dfa.columns:
        dfa = dfa.withColumn(i, when(col(i) == None,'').otherwise(col(i)))

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node User
User_node1656656987136 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_user",
    transformation_ctx="User_node1656656987136",
)

# Script generated for node Order
Order_node1656656953215 = glueContext.create_dynamic_frame.from_catalog(
    database="dev-sf-db",
    table_name="salesforce_order",
    transformation_ctx="Order_node1656656953215",
)

# Script generated for node Renamed keys for OrderJoinsUser
RenamedkeysforOrderJoinsUser_node1656657064384 = ApplyMapping.apply(
    frame=User_node1656656987136,
    mappings=[
        ("id", "string", "`(usr) id`", "string"),
        ("username", "string", "`(usr) username`", "string"),
        ("lastname", "string", "`(usr) lastname`", "string"),
        ("firstname", "string", "`(usr) firstname`", "string"),
        ("middlename", "string", "`(usr) middlename`", "string"),
        ("suffix", "string", "`(usr) suffix`", "string"),
        ("name", "string", "`(usr) name`", "string"),
        ("companyname", "string", "`(usr) companyname`", "string"),
        ("division", "string", "`(usr) division`", "string"),
        ("department", "string", "`(usr) department`", "string"),
        ("title", "string", "`(usr) title`", "string"),
        ("street", "string", "`(usr) street`", "string"),
        ("city", "string", "`(usr) city`", "string"),
        ("state", "string", "`(usr) state`", "string"),
        ("postalcode", "string", "`(usr) postalcode`", "string"),
        ("country", "string", "`(usr) country`", "string"),
        ("latitude", "string", "`(usr) latitude`", "string"),
        ("longitude", "string", "`(usr) longitude`", "string"),
        ("geocodeaccuracy", "string", "`(usr) geocodeaccuracy`", "string"),
        ("address", "string", "`(usr) address`", "string"),
        ("email", "string", "`(usr) email`", "string"),
        (
            "emailpreferencesautobcc",
            "string",
            "`(usr) emailpreferencesautobcc`",
            "string",
        ),
        (
            "emailpreferencesautobccstayintouch",
            "string",
            "`(usr) emailpreferencesautobccstayintouch`",
            "string",
        ),
        (
            "emailpreferencesstayintouchreminder",
            "string",
            "`(usr) emailpreferencesstayintouchreminder`",
            "string",
        ),
        ("senderemail", "string", "`(usr) senderemail`", "string"),
        ("sendername", "string", "`(usr) sendername`", "string"),
        ("signature", "string", "`(usr) signature`", "string"),
        ("stayintouchsubject", "string", "`(usr) stayintouchsubject`", "string"),
        ("stayintouchsignature", "string", "`(usr) stayintouchsignature`", "string"),
        ("stayintouchnote", "string", "`(usr) stayintouchnote`", "string"),
        ("phone", "string", "`(usr) phone`", "string"),
        ("fax", "string", "`(usr) fax`", "string"),
        ("mobilephone", "string", "`(usr) mobilephone`", "string"),
        ("alias", "string", "`(usr) alias`", "string"),
        ("communitynickname", "string", "`(usr) communitynickname`", "string"),
        ("badgetext", "string", "`(usr) badgetext`", "string"),
        ("isactive", "string", "`(usr) isactive`", "string"),
        ("timezonesidkey", "string", "`(usr) timezonesidkey`", "string"),
        ("userroleid", "string", "`(usr) userroleid`", "string"),
        ("localesidkey", "string", "`(usr) localesidkey`", "string"),
        ("receivesinfoemails", "string", "`(usr) receivesinfoemails`", "string"),
        (
            "receivesadmininfoemails",
            "string",
            "`(usr) receivesadmininfoemails`",
            "string",
        ),
        ("emailencodingkey", "string", "`(usr) emailencodingkey`", "string"),
        ("profileid", "string", "`(usr) profileid`", "string"),
        ("usertype", "string", "`(usr) usertype`", "string"),
        ("languagelocalekey", "string", "`(usr) languagelocalekey`", "string"),
        ("employeenumber", "string", "`(usr) employeenumber`", "string"),
        ("delegatedapproverid", "string", "`(usr) delegatedapproverid`", "string"),
        ("managerid", "string", "`(usr) managerid`", "string"),
        ("lastlogindate", "string", "`(usr) lastlogindate`", "string"),
        (
            "lastpasswordchangedate",
            "string",
            "`(usr) lastpasswordchangedate`",
            "string",
        ),
        ("createddate", "string", "`(usr) createddate`", "string"),
        ("createdbyid", "string", "`(usr) createdbyid`", "string"),
        ("lastmodifieddate", "string", "`(usr) lastmodifieddate`", "string"),
        ("lastmodifiedbyid", "string", "`(usr) lastmodifiedbyid`", "string"),
        ("systemmodstamp", "string", "`(usr) systemmodstamp`", "string"),
        ("numberoffailedlogins", "string", "`(usr) numberoffailedlogins`", "string"),
        (
            "offlinetrialexpirationdate",
            "string",
            "`(usr) offlinetrialexpirationdate`",
            "string",
        ),
        (
            "offlinepdatrialexpirationdate",
            "string",
            "`(usr) offlinepdatrialexpirationdate`",
            "string",
        ),
        (
            "userpermissionsmarketinguser",
            "string",
            "`(usr) userpermissionsmarketinguser`",
            "string",
        ),
        (
            "userpermissionsofflineuser",
            "string",
            "`(usr) userpermissionsofflineuser`",
            "string",
        ),
        (
            "userpermissionsavantgouser",
            "string",
            "`(usr) userpermissionsavantgouser`",
            "string",
        ),
        (
            "userpermissionscallcenterautologin",
            "string",
            "`(usr) userpermissionscallcenterautologin`",
            "string",
        ),
        (
            "userpermissionssfcontentuser",
            "string",
            "`(usr) userpermissionssfcontentuser`",
            "string",
        ),
        (
            "userpermissionsknowledgeuser",
            "string",
            "`(usr) userpermissionsknowledgeuser`",
            "string",
        ),
        (
            "userpermissionsinteractionuser",
            "string",
            "`(usr) userpermissionsinteractionuser`",
            "string",
        ),
        (
            "userpermissionssupportuser",
            "string",
            "`(usr) userpermissionssupportuser`",
            "string",
        ),
        ("forecastenabled", "string", "`(usr) forecastenabled`", "string"),
        (
            "userpreferencesactivityreminderspopup",
            "string",
            "`(usr) userpreferencesactivityreminderspopup`",
            "string",
        ),
        (
            "userpreferenceseventreminderscheckboxdefault",
            "string",
            "`(usr) userpreferenceseventreminderscheckboxdefault`",
            "string",
        ),
        (
            "userpreferencestaskreminderscheckboxdefault",
            "string",
            "`(usr) userpreferencestaskreminderscheckboxdefault`",
            "string",
        ),
        (
            "userpreferencesremindersoundoff",
            "string",
            "`(usr) userpreferencesremindersoundoff`",
            "string",
        ),
        (
            "userpreferencesdisableallfeedsemail",
            "string",
            "`(usr) userpreferencesdisableallfeedsemail`",
            "string",
        ),
        (
            "userpreferencesdisablefollowersemail",
            "string",
            "`(usr) userpreferencesdisablefollowersemail`",
            "string",
        ),
        (
            "userpreferencesdisableprofilepostemail",
            "string",
            "`(usr) userpreferencesdisableprofilepostemail`",
            "string",
        ),
        (
            "userpreferencesdisablechangecommentemail",
            "string",
            "`(usr) userpreferencesdisablechangecommentemail`",
            "string",
        ),
        (
            "userpreferencesdisablelatercommentemail",
            "string",
            "`(usr) userpreferencesdisablelatercommentemail`",
            "string",
        ),
        (
            "userpreferencesdisprofpostcommentemail",
            "string",
            "`(usr) userpreferencesdisprofpostcommentemail`",
            "string",
        ),
        (
            "userpreferencescontentnoemail",
            "string",
            "`(usr) userpreferencescontentnoemail`",
            "string",
        ),
        (
            "userpreferencescontentemailasandwhen",
            "string",
            "`(usr) userpreferencescontentemailasandwhen`",
            "string",
        ),
        (
            "userpreferencesapexpagesdevelopermode",
            "string",
            "`(usr) userpreferencesapexpagesdevelopermode`",
            "string",
        ),
        (
            "userpreferencesreceivenonotificationsasapprover",
            "string",
            "`(usr) userpreferencesreceivenonotificationsasapprover`",
            "string",
        ),
        (
            "userpreferencesreceivenotificationsasdelegatedapprover",
            "string",
            "`(usr) userpreferencesreceivenotificationsasdelegatedapprover`",
            "string",
        ),
        (
            "userpreferenceshidecsngetchattermobiletask",
            "string",
            "`(usr) userpreferenceshidecsngetchattermobiletask`",
            "string",
        ),
        (
            "userpreferencesdisablementionspostemail",
            "string",
            "`(usr) userpreferencesdisablementionspostemail`",
            "string",
        ),
        (
            "userpreferencesdismentionscommentemail",
            "string",
            "`(usr) userpreferencesdismentionscommentemail`",
            "string",
        ),
        (
            "userpreferenceshidecsndesktoptask",
            "string",
            "`(usr) userpreferenceshidecsndesktoptask`",
            "string",
        ),
        (
            "userpreferenceshidechatteronboardingsplash",
            "string",
            "`(usr) userpreferenceshidechatteronboardingsplash`",
            "string",
        ),
        (
            "userpreferenceshidesecondchatteronboardingsplash",
            "string",
            "`(usr) userpreferenceshidesecondchatteronboardingsplash`",
            "string",
        ),
        (
            "userpreferencesdiscommentafterlikeemail",
            "string",
            "`(usr) userpreferencesdiscommentafterlikeemail`",
            "string",
        ),
        (
            "userpreferencesdisablelikeemail",
            "string",
            "`(usr) userpreferencesdisablelikeemail`",
            "string",
        ),
        (
            "userpreferencessortfeedbycomment",
            "string",
            "`(usr) userpreferencessortfeedbycomment`",
            "string",
        ),
        (
            "userpreferencesdisablemessageemail",
            "string",
            "`(usr) userpreferencesdisablemessageemail`",
            "string",
        ),
        (
            "userpreferencesdisablebookmarkemail",
            "string",
            "`(usr) userpreferencesdisablebookmarkemail`",
            "string",
        ),
        (
            "userpreferencesdisablesharepostemail",
            "string",
            "`(usr) userpreferencesdisablesharepostemail`",
            "string",
        ),
        (
            "userpreferencesenableautosubforfeeds",
            "string",
            "`(usr) userpreferencesenableautosubforfeeds`",
            "string",
        ),
        (
            "userpreferencesdisablefilesharenotificationsforapi",
            "string",
            "`(usr) userpreferencesdisablefilesharenotificationsforapi`",
            "string",
        ),
        (
            "userpreferencesshowtitletoexternalusers",
            "string",
            "`(usr) userpreferencesshowtitletoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowmanagertoexternalusers",
            "string",
            "`(usr) userpreferencesshowmanagertoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowemailtoexternalusers",
            "string",
            "`(usr) userpreferencesshowemailtoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowworkphonetoexternalusers",
            "string",
            "`(usr) userpreferencesshowworkphonetoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowmobilephonetoexternalusers",
            "string",
            "`(usr) userpreferencesshowmobilephonetoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowfaxtoexternalusers",
            "string",
            "`(usr) userpreferencesshowfaxtoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowstreetaddresstoexternalusers",
            "string",
            "`(usr) userpreferencesshowstreetaddresstoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowcitytoexternalusers",
            "string",
            "`(usr) userpreferencesshowcitytoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowstatetoexternalusers",
            "string",
            "`(usr) userpreferencesshowstatetoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowpostalcodetoexternalusers",
            "string",
            "`(usr) userpreferencesshowpostalcodetoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowcountrytoexternalusers",
            "string",
            "`(usr) userpreferencesshowcountrytoexternalusers`",
            "string",
        ),
        (
            "userpreferencesshowprofilepictoguestusers",
            "string",
            "`(usr) userpreferencesshowprofilepictoguestusers`",
            "string",
        ),
        (
            "userpreferencesshowtitletoguestusers",
            "string",
            "`(usr) userpreferencesshowtitletoguestusers`",
            "string",
        ),
        (
            "userpreferencesshowcitytoguestusers",
            "string",
            "`(usr) userpreferencesshowcitytoguestusers`",
            "string",
        ),
        (
            "userpreferencesshowstatetoguestusers",
            "string",
            "`(usr) userpreferencesshowstatetoguestusers`",
            "string",
        ),
        (
            "userpreferencesshowpostalcodetoguestusers",
            "string",
            "`(usr) userpreferencesshowpostalcodetoguestusers`",
            "string",
        ),
        (
            "userpreferencesshowcountrytoguestusers",
            "string",
            "`(usr) userpreferencesshowcountrytoguestusers`",
            "string",
        ),
        (
            "userpreferenceshideinvoicesredirectconfirmation",
            "string",
            "`(usr) userpreferenceshideinvoicesredirectconfirmation`",
            "string",
        ),
        (
            "userpreferenceshidestatementsredirectconfirmation",
            "string",
            "`(usr) userpreferenceshidestatementsredirectconfirmation`",
            "string",
        ),
        (
            "userpreferenceshides1browserui",
            "string",
            "`(usr) userpreferenceshides1browserui`",
            "string",
        ),
        (
            "userpreferencesdisableendorsementemail",
            "string",
            "`(usr) userpreferencesdisableendorsementemail`",
            "string",
        ),
        (
            "userpreferencespathassistantcollapsed",
            "string",
            "`(usr) userpreferencespathassistantcollapsed`",
            "string",
        ),
        (
            "userpreferencescachediagnostics",
            "string",
            "`(usr) userpreferencescachediagnostics`",
            "string",
        ),
        (
            "userpreferencesshowemailtoguestusers",
            "string",
            "`(usr) userpreferencesshowemailtoguestusers`",
            "string",
        ),
        (
            "userpreferencesshowmanagertoguestusers",
            "string",
            "`(usr) userpreferencesshowmanagertoguestusers`",
            "string",
        ),
        (
            "userpreferencesshowworkphonetoguestusers",
            "string",
            "`(usr) userpreferencesshowworkphonetoguestusers`",
            "string",
        ),
        (
            "userpreferencesshowmobilephonetoguestusers",
            "string",
            "`(usr) userpreferencesshowmobilephonetoguestusers`",
            "string",
        ),
        (
            "userpreferencesshowfaxtoguestusers",
            "string",
            "`(usr) userpreferencesshowfaxtoguestusers`",
            "string",
        ),
        (
            "userpreferencesshowstreetaddresstoguestusers",
            "string",
            "`(usr) userpreferencesshowstreetaddresstoguestusers`",
            "string",
        ),
        (
            "userpreferenceslightningexperiencepreferred",
            "string",
            "`(usr) userpreferenceslightningexperiencepreferred`",
            "string",
        ),
        (
            "userpreferencespreviewlightning",
            "string",
            "`(usr) userpreferencespreviewlightning`",
            "string",
        ),
        (
            "userpreferenceshideenduseronboardingassistantmodal",
            "string",
            "`(usr) userpreferenceshideenduseronboardingassistantmodal`",
            "string",
        ),
        (
            "userpreferenceshidelightningmigrationmodal",
            "string",
            "`(usr) userpreferenceshidelightningmigrationmodal`",
            "string",
        ),
        (
            "userpreferenceshidesfxwelcomemat",
            "string",
            "`(usr) userpreferenceshidesfxwelcomemat`",
            "string",
        ),
        (
            "userpreferenceshidebiggerphotocallout",
            "string",
            "`(usr) userpreferenceshidebiggerphotocallout`",
            "string",
        ),
        (
            "userpreferencesglobalnavbarwtshown",
            "string",
            "`(usr) userpreferencesglobalnavbarwtshown`",
            "string",
        ),
        (
            "userpreferencesglobalnavgridmenuwtshown",
            "string",
            "`(usr) userpreferencesglobalnavgridmenuwtshown`",
            "string",
        ),
        (
            "userpreferencescreatelexappswtshown",
            "string",
            "`(usr) userpreferencescreatelexappswtshown`",
            "string",
        ),
        (
            "userpreferencesfavoriteswtshown",
            "string",
            "`(usr) userpreferencesfavoriteswtshown`",
            "string",
        ),
        (
            "userpreferencesrecordhomesectioncollapsewtshown",
            "string",
            "`(usr) userpreferencesrecordhomesectioncollapsewtshown`",
            "string",
        ),
        (
            "userpreferencesrecordhomereservedwtshown",
            "string",
            "`(usr) userpreferencesrecordhomereservedwtshown`",
            "string",
        ),
        (
            "userpreferencesfavoritesshowtopfavorites",
            "string",
            "`(usr) userpreferencesfavoritesshowtopfavorites`",
            "string",
        ),
        (
            "userpreferencesexcludemailappattachments",
            "string",
            "`(usr) userpreferencesexcludemailappattachments`",
            "string",
        ),
        (
            "userpreferencessuppresstasksfxreminders",
            "string",
            "`(usr) userpreferencessuppresstasksfxreminders`",
            "string",
        ),
        (
            "userpreferencessuppresseventsfxreminders",
            "string",
            "`(usr) userpreferencessuppresseventsfxreminders`",
            "string",
        ),
        (
            "userpreferencespreviewcustomtheme",
            "string",
            "`(usr) userpreferencespreviewcustomtheme`",
            "string",
        ),
        (
            "userpreferenceshascelebrationbadge",
            "string",
            "`(usr) userpreferenceshascelebrationbadge`",
            "string",
        ),
        (
            "userpreferencesuserdebugmodepref",
            "string",
            "`(usr) userpreferencesuserdebugmodepref`",
            "string",
        ),
        (
            "userpreferencessrhoverrideactivities",
            "string",
            "`(usr) userpreferencessrhoverrideactivities`",
            "string",
        ),
        (
            "userpreferencesnewlightningreportrunpageenabled",
            "string",
            "`(usr) userpreferencesnewlightningreportrunpageenabled`",
            "string",
        ),
        (
            "userpreferencesnativeemailclient",
            "string",
            "`(usr) userpreferencesnativeemailclient`",
            "string",
        ),
        (
            "userpreferenceshidebrowseproductredirectconfirmation",
            "string",
            "`(usr) userpreferenceshidebrowseproductredirectconfirmation`",
            "string",
        ),
        ("contactid", "string", "`(usr) contactid`", "string"),
        ("accountid", "string", "`(usr) accountid`", "string"),
        ("callcenterid", "string", "`(usr) callcenterid`", "string"),
        ("extension", "string", "`(usr) extension`", "string"),
        ("federationidentifier", "string", "`(usr) federationidentifier`", "string"),
        ("aboutme", "string", "`(usr) aboutme`", "string"),
        ("fullphotourl", "string", "`(usr) fullphotourl`", "string"),
        ("smallphotourl", "string", "`(usr) smallphotourl`", "string"),
        ("isextindicatorvisible", "string", "`(usr) isextindicatorvisible`", "string"),
        ("outofofficemessage", "string", "`(usr) outofofficemessage`", "string"),
        ("mediumphotourl", "string", "`(usr) mediumphotourl`", "string"),
        ("digestfrequency", "string", "`(usr) digestfrequency`", "string"),
        (
            "defaultgroupnotificationfrequency",
            "string",
            "`(usr) defaultgroupnotificationfrequency`",
            "string",
        ),
        ("lastvieweddate", "string", "`(usr) lastvieweddate`", "string"),
        ("lastreferenceddate", "string", "`(usr) lastreferenceddate`", "string"),
        ("bannerphotourl", "string", "`(usr) bannerphotourl`", "string"),
        ("smallbannerphotourl", "string", "`(usr) smallbannerphotourl`", "string"),
        ("mediumbannerphotourl", "string", "`(usr) mediumbannerphotourl`", "string"),
        ("isprofilephotoactive", "string", "`(usr) isprofilephotoactive`", "string"),
        ("individualid", "string", "`(usr) individualid`", "string"),
        (
            "vlocity_cmt__docusignemail__c",
            "string",
            "`(usr) vlocity_cmt__docusignemail__c`",
            "string",
        ),
        (
            "vlocity_cmt__globalkey__c",
            "string",
            "`(usr) vlocity_cmt__globalkey__c`",
            "string",
        ),
        (
            "vlocity_cmt__omplussyncenabled__c",
            "string",
            "`(usr) vlocity_cmt__omplussyncenabled__c`",
            "string",
        ),
        (
            "vlocity_cmt__omplus_enabled__c",
            "string",
            "`(usr) vlocity_cmt__omplus_enabled__c`",
            "string",
        ),
        (
            "vlocity_cmt__storeassociatestatus__c",
            "string",
            "`(usr) vlocity_cmt__storeassociatestatus__c`",
            "string",
        ),
        ("filename", "string", "`(usr) filename`", "string"),
        ("function_name", "string", "`(usr) function_name`", "string"),
        ("function_version", "string", "`(usr) function_version`", "string"),
        ("aws_request_id", "string", "`(usr) aws_request_id`", "string"),
        ("appflow_id", "string", "`(usr) appflow_id`", "string"),
        ("processed_ts", "timestamp", "`(usr) processed_ts`", "timestamp"),
        ("usergroup__c", "string", "`(usr) usergroup__c`", "string"),
        (
            "userpermissionsliveagentuser",
            "string",
            "`(usr) userpermissionsliveagentuser`",
            "string",
        ),
        ("sales_office__c", "string", "`(usr) sales_office__c`", "string"),
        (
            "neilon__is_s3_link_user__c",
            "string",
            "`(usr) neilon__is_s3_link_user__c`",
            "string",
        ),
        ("partition_0", "string", "`(usr) partition_0`", "string"),
        ("partition_1", "string", "`(usr) partition_1`", "string"),
        ("partition_2", "string", "`(usr) partition_2`", "string"),
        ("partition_3", "string", "`(usr) partition_3`", "string"),
    ],
    transformation_ctx="RenamedkeysforOrderJoinsUser_node1656657064384",
)

# Script generated for node OrderJoinsUser
OrderJoinsUser_node1656657029141 = Join.apply(
    frame1=Order_node1656656953215,
    frame2=RenamedkeysforOrderJoinsUser_node1656657064384,
    keys1=["createdbyid"],
    keys2=["`(usr) id`"],
    transformation_ctx="OrderJoinsUser_node1656657029141",
)

# Script generated for node Apply Mapping
ApplyMapping_node1656657098948 = ApplyMapping.apply(
    frame=OrderJoinsUser_node1656657029141,
    mappings=[
        ("id", "string", "Sales_Order_ID", "string"),
        (
            "vlocity_cmt__masterordername__c",
            "string",
            "Sales_Order_Name",
            "string",
        ),
        ("`(usr) id`", "string", "User_ID", "string"),
        ("`(usr) username`", "string", "User_Name", "string"),
        ("`(usr) userroleid`", "string", "Functional_Role_Name", "string"),
    ],
    transformation_ctx="ApplyMapping_node1656657098948",
)

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = ApplyMapping_node1656657098948.toDF().coalesce(1)

# Added columns not available in Saleforce
partitioned_dataframe = partitioned_dataframe \
            .withColumn("User_Login",lit(None).cast('string')) \
            .withColumn("Functional_Role_Internal_Name",lit(None).cast('string')) \
            .withColumn("Functional_Role_ID",lit(None).cast('string')) \
            .withColumn("Commission_Split",lit(None).cast('string')) \
            .withColumn("OP1_Sales_Order_ID",lit(None)) \
            .withColumn("OP1_User_ID",lit(None)) \
            .withColumn("OP1_Functional_Role_ID",lit(None))

partitioned_dataframe = convertToEmptyString(partitioned_dataframe)

# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

# Script generated for node Amazon S3
Synth_Sales_Order_Users_Map = glueContext.write_dynamic_frame.from_options(
    frame=partitioned_dynamicframe,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://swmi-etldev-sf-to-dw-dwmock",
        "partitionKeys": [],
    },
    transformation_ctx="Synth_Sales_Order_Users_Map",
)

job.commit()


###############################################################
## Clowning the existing file and deleting the old file.    ###
###############################################################

bucket_name = "swmi-etldev-sf-to-dw-dwmock"
filename = "Synth-Sales_Order_Users_Map" + "_" + str(datetime.now().strftime("%d%m%Y"))

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
