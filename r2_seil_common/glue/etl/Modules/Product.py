import boto3
import io
import pandas as pd
import numpy as np
import s3fs
import logging
import sys
from datetime import datetime
from datetime import date
import datetime
from pathlib import Path
from io import StringIO

from boto3.dynamodb.conditions import Key, Attr
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import when, lit
from pyspark.sql.types import StringType
import pyspark.sql.functions as func

#####TransformOP1ObjectsSFProductV1.txt################
#####Processing for Product SF module (4 objects) to OP1 objects
 
if __name__ == '__main__':
    ProcessGlueCode = 1
    CSVorParquetRun = 2
    InputBucket = "swmi-etldev-sf-to-dw-staging"
    InBucket = "swmi-etldev-sf-to-dw-curated"
    OutBucket = "swmi-etldev-sf-to-dw-dwmock"
   
    try: 
     if (ProcessGlueCode == 1):  
        s3_client = boto3.client('s3') 
        s3_resrc = boto3.resource('s3')
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
		
        session_dest = boto3.session.Session(aws_access_key_id="[REDACTED_AWS_ACCESS_KEY]",
                                         region_name="ap-southeast-2",
                                         aws_secret_access_key="[REDACTED_AWS_SECRET_KEY]")
        dest_s3_r = session_dest.resource('s3')
		
		#####Read ProdCode Map file for SF to OP1 need in future to get OP1 product id
        csv_file_name = "SFProd2OP1Map.csv"
        csv_object = s3_client.get_object(Bucket=InputBucket,Key=csv_file_name)
        file_reader = csv_object['Body'].read().decode("utf-8",errors='replace')
        users = file_reader.split("\n")
        users = list(filter(None, users))
        Mapdata = list()
        for cntr, user in enumerate(users):
          if (cntr>0):    
             user_data = user.split(",")
             d = dict()  
             d['OP1Name'] = user_data[0]
             d['Product_ID'] = user_data[1]
             d['SFFormat'] = user_data[2]
             Mapdata.append(d)		
        print('MapVal', type(Mapdata))
		
        #run by Sachhi 	'Product2', 'vlocity_cmt__AdSpaceSpecification__c', 'vlocity_cmt__AdSpaceCreativeSizeType__c', 'vlocity_cmt__AdCreativeSizeType__c'
        SFUnqTableNames = ['Product', 'Ad-Space-Specification', 'Ad-Space-Creative-SizeType','Ad-Creative-SizeType']
        #SFUnqTableNames = ['Product','Ad_Space_Specification','Ad_Space_Creative_Size_Type','Ad_Creative_Size_Type']
        pt1 = False
        pt2 = False
        
        #print('Glue Job Starts for transformation of SF Obj:', SFUnqTableNames)
        for runNo, SFObject in enumerate(SFUnqTableNames):
          print('Processing for SFObject::', SFObject)
		  
		  #####Listing all CSV files for a SF Object  
          bucket = s3_resrc.Bucket(InputBucket)
          FileList=[]
          #for key in bucket.objects.all():
          for obj in bucket.objects.filter(Prefix=SFObject):
            if obj.key.endswith('csv'):
               FileList.append(obj.key)
          print('Input files::',FileList)  
		  
		  #####Listing all Parquet files for a SF Object  
          SFObjLocation = 'Salesforce-' + SFObject + '/'
          bucket = s3_resrc.Bucket(InBucket)
          PFileList=[]
          PPFileList=[]
          #for key in bucket.objects.all():
          for obj in bucket.objects.filter(Prefix=SFObjLocation):
            if obj.key.endswith('parquet'):
               PPFileList.append(obj.key)
          print('Input Parquet files::',PPFileList)  		  
          print('Input Parquet files #::',len(PPFileList)) 
		  
		  #For quick run.......
          for cntr, aitem in enumerate(PPFileList):
             if (cntr >= 0):    
                PFileList.append(aitem)
          print('Input Parquet files::',PFileList)  		  
          print('Input Parquet files #::',len(PFileList)) 		  
		  
		  #####Finding Latest Date for a SF Obj from available Parquet files
          LatestDate=''		  
          if (len(PFileList)==0):
           print('Input SF Objects missing from Curated bucket!!!', SFObjLocation)
          else:
           Latest = 99
           LatestDate=''
           Currdate=datetime.datetime.now()
           format = "%Y-%m-%d"
           for aFile in PFileList:
             aDatePart = aFile.split('/')[1] + '-' + aFile.split('/') [2] + '-' + aFile.split('/')[3]
             dt_object = datetime.datetime.strptime(aDatePart, format)
             diff = (Currdate - dt_object).days
             if (Latest > diff):
                Latest = diff
                LatestDate = aDatePart          

          #####Finding Latest File List Date for a SF Obj from available Parquet files		  
          LatestFileList=[]      
          if (LatestDate != ''):
             print('LatestDate', LatestDate)
             for aFile in PFileList:
                 aDatePart = aFile.split('/')[1] + '-' + aFile.split('/') [2] + '-' + aFile.split('/')[3]
                 if (aDatePart == LatestDate):
                    LatestFileList.append(aFile) 

		  
          #####DDMMYYYYFinding Latest schema for a SF Obj from available Parquet files   df = spark.read.format("text").load("output.txt")
          #LatestDate = '06072022'
          if (LatestDate != ''):
            if (CSVorParquetRun == 2):                
               InKey = LatestFileList[0] 
               SFdfLatst = spark.read.options(header='True', inferSchema='True').format('parquet').load('s3://' + InBucket + '/' + InKey)
               LatestSchma =  SFdfLatst.schema
               #####Control for all files with latest schema/latest files only for a date         
		       #WorkingFileList =  LatestFileList
               WorkingFileList =  PFileList 		  
		  
		    ####For CSV read from S3 stage folder
            if (CSVorParquetRun == 1):
               WorkingFileList =  FileList   
          
            print('WorkingFileList::',WorkingFileList) 

            ####For parquet file read
            if (CSVorParquetRun == 2):			
               noOfVldFiles = 0            
               for noOfFiles, aFile in enumerate(WorkingFileList):
                 InKey = aFile
                 SFdfTemp = spark.read.options(header='True', inferSchema='True').format('parquet').load('s3://' + InBucket + '/' + InKey)
                 CurrSchma =  SFdfTemp.schema
                 if (set(LatestSchma)-set(CurrSchma)==set()) and (set(CurrSchma)-set(LatestSchma)==set()):
                    noOfVldFiles = noOfVldFiles + 1 
                    if (noOfVldFiles == 1):
                       SFdf = SFdfTemp
                    if (noOfVldFiles > 1):
                       SFdf = SFdf.union(SFdfTemp)
               print('Effective Merged Files no', noOfVldFiles) 			
			
				
			####For CSV read 
            if (CSVorParquetRun == 1):
              for noOfFiles, aFile in enumerate(WorkingFileList):			  
                InKey = aFile
                SFdf = spark.read.options(header='True', inferSchema='True').format('csv').load('s3://' + InputBucket + '/' + InKey)
             			 
                
            InputSFColumnNames = SFdf.columns
            print('InputSFColumnNames', InputSFColumnNames)
            print('InputSFColumnCount', len(InputSFColumnNames))
            print('InputSFRowCount', SFdf.count())
			#Ad Space Creative SizeType API---vlocity_cmt__AdSpaceCreativeSizeType__c S3--Salesforce-Ad_Space_Creative_Size_Type
			#Ad Space Specification	API---vlocity_cmt__AdSpaceSpecification__c   S3--Salesforce-Ad_Space_Specification
			#Ad Creative SizeType API---	vlocity_cmt__AdCreativeSizeType__c   S3--Salesforce-Ad_Creative_Size_Type
			#Product	API--  Product2    Salesforce-Product
			#SFUnqTableNames = ['Product2', 'Ad-Space-Specification  adSpcSpecfy', 'Ad-Space-Creative-Size-Type   adSpcrsztyp','Ad-Creative-Size-Type  adcrsztyp']
            #SFUnqTableNames = ['Product','Ad_Space_Specification','Ad_Space_Creative_Size_Type','Ad_Creative_Size_Type']
            #filling with blank
            SFdf.na.fill('')
            
            if (SFObject == 'Ad-Space-Creative-SizeType'):
                adSpcrsztypT = SFdf
                pt1 = True
                print('Ad-Space-Creative-SizeType', adSpcrsztypT.count())
                adSpcrsztyp=adSpcrsztypT.orderBy('processed_ts').dropDuplicates(subset = ['Id'])
                print('Ad-Space-Creative-SizeType Latest', adSpcrsztyp.count())
                
            if (SFObject == 'Ad-Creative-SizeType'):
                adcrsztypT = SFdf      
                pt2 = True
                print('Ad-Creative-SizeType', adcrsztypT.count()) 
                adcrsztypT1=adcrsztypT.orderBy('processed_ts').dropDuplicates(subset = ['Id'])
				
                #adcrsztypT2 = adcrsztypT1.withColumn("vlocity_cmt__Width__c",adcrsztypT1["vlocity_cmt__Width__c"].cast(StringType()))
                #adcrsztypT3 = adcrsztypT2.withColumn("vlocity_cmt__Height__c",adcrsztypT2["vlocity_cmt__Height__c"].cast(StringType()))
                #adcrsztyp = adcrsztypT3.withColumn("vlocity_cmt__RunTime__c",adcrsztypT3["vlocity_cmt__RunTime__c"].cast(StringType()))
				
				#df2 = df.withColumn("col4", func.round(df["col3"], 2).cast('integer'))
                adcrsztypT2 = adcrsztypT1.withColumn("vlocity_cmt__Width__c",func.round(adcrsztypT1["vlocity_cmt__Width__c"], 2).cast('integer'))
                adcrsztypT3 = adcrsztypT2.withColumn("vlocity_cmt__Height__c",func.round(adcrsztypT2["vlocity_cmt__Height__c"], 2).cast('integer'))
                adcrsztyp = adcrsztypT3.withColumn("vlocity_cmt__RunTime__c",func.round(adcrsztypT3["vlocity_cmt__RunTime__c"], 2).cast('integer'))				
                 
            if (SFObject == 'Ad-Space-Specification'):
                adSpcSpecfyT = SFdf      
                pt3 = True
                print('Ad-Space-Specification', adSpcSpecfyT.count())
                print('Ad-Space-Specification', adSpcSpecfyT.columns)
                adSpcSpecfyTT=adSpcSpecfyT.orderBy('processed_ts').dropDuplicates(subset = ['Id'])	
				#insert a col based on other column value....
				
                #adSpcSpecfy=adSpcSpecfy.withColumn("Device__c", adSpcSpecfy.when(adSpcSpecfy.col("Device__c").isNull(), '').otherwise(adSpcSpecfy.col("Device__c")))
                adSpcSpecfy = adSpcSpecfyTT.withColumn("Device__c", when(adSpcSpecfyTT.Name.contains("_CTV_"),"CTV").otherwise(''))
                
                 
            if (SFObject == 'Product'):
                print('Product2BfrCorrc', SFdf.count())
                SFdf=SFdf.drop(*['vlocity_cmt__AttributeMetadata__c'])
                SFdf=SFdf.drop(*['vlocity_cmt__JSONAttribute__c'])
                Product2T = SFdf     
                pt4 = True
                print('Product2', Product2T.count())
                Product2=Product2T.orderBy('processed_ts').dropDuplicates(subset = ['Id'])				
                
        #####left join on Four dataframes  
        if (pt1)and(pt2)and(pt3)and(pt4):
             NewColmns = []
             Colmns = adSpcrsztyp.columns
             for acol in Colmns:
                 NewColmns.append('adSpcrsztyp'+acol)
 
             adSpcrsztyp = adSpcrsztyp.toDF(*NewColmns)
             NColmns = adSpcrsztyp.columns    
             print('NewColmnsAdSpace', NColmns)  
             
             NewColmns = []
             Colmns = adcrsztyp.columns      
             for acol in Colmns:
                 NewColmns.append('adcrsztyp'+acol)
             adcrsztyp = adcrsztyp.toDF(*NewColmns)
             NColmns = adcrsztyp.columns    
             print('NewColmnsAdSizeTyp', NColmns)        
       
             NewColmns = []
             Colmns = adSpcSpecfy.columns      
             for acol in Colmns:
                 NewColmns.append('adSpcSpecfy'+acol)
             adSpcSpecfy = adSpcSpecfy.toDF(*NewColmns)
             NColmns = adSpcSpecfy.columns    
             print('NewColmnsAdSpecification', NColmns)             
             
			 #Ad Space Creative Size Type and Ad Creative Size Type
             joinedDF1 = adSpcrsztyp.join(adcrsztyp, adSpcrsztyp.adSpcrsztypvlocity_cmt__AdCreativeSizeTypeId__c == adcrsztyp.adcrsztypId, "leftouter") 
             JoinedSFColumnNames = joinedDF1.columns
             print('Joined1SFColumnNames', JoinedSFColumnNames)
             
             #Left Outer
             #joinedDF2 = joinedDF1.join(adSpcSpecfy, joinedDF1.adSpcrsztypvlocity_cmt__AdSpaceSpecificationId__c == adSpcSpecfy.adSpcSpecfyId, "leftouter") 
            
             #Outer
             joinedDF2 = joinedDF1.join(adSpcSpecfy, joinedDF1.adSpcrsztypvlocity_cmt__AdSpaceSpecificationId__c == adSpcSpecfy.adSpcSpecfyId, "outer") 

             JoinedSFColumnNames = joinedDF2.columns
             print('Joined2SFColumnNames', JoinedSFColumnNames)

             #Outer
             joinedDF3 = Product2.join(joinedDF2, joinedDF2.adSpcSpecfyvlocity_cmt__Product2Id__c == Product2.Id, "leftouter") 

             JoinedSFColumnNames = joinedDF3.columns
             print('Joined3SFColumnNames', JoinedSFColumnNames)    
             
             #Creating OP1 product name......
             #when((df.salary >= 4000) & (df.salary <= 5000),  adcrsztypvlocity_cmt__MediaType__c
			 #integer to string conversion.....		 
			 
             joinedDF3 = joinedDF3.withColumn("name1", when((joinedDF3.adcrsztypvlocity_cmt__Width__c > 0) & (joinedDF3.adcrsztypvlocity_cmt__Height__c > 0), concat_ws("x","adcrsztypvlocity_cmt__Width__c",'adcrsztypvlocity_cmt__Height__c')))
             joinedDF3 = joinedDF3.withColumn("namedmmy", when((joinedDF3.adcrsztypvlocity_cmt__MediaType__c =='Digital Video'), lit('v')))             
             joinedDF3 = joinedDF3.withColumn("name2", concat_ws("","name1",'namedmmy'))
           
		     # joinedDF3.adSpcSpecfyDevice__c   location changed...07/09/2022
             joinedDF3 = joinedDF3.withColumn("name3", when((joinedDF3.adcrsztypvlocity_cmt__MediaType__c == 'Digital Video') & (joinedDF3.adSpcSpecfyDevice__c != ''), concat_ws(" | ","adSpcSpecfyDevice__c",'name2')).otherwise(joinedDF3.name2))
             joinedDF3 = joinedDF3.withColumn("name4", when((joinedDF3.adcrsztypvlocity_cmt__RunTime__c > 0), concat_ws(" | ","adcrsztypvlocity_cmt__RunTime__c",'name3')).otherwise(joinedDF3.name3))
             joinedDF3 = joinedDF3.withColumn("Product_Name", when((joinedDF3.Name != '') & (joinedDF3.name4 != '') , concat_ws(" ","Name",'name4')).otherwise(joinedDF3.Name))
            
             #Merge rows with same name with different display for Digital Banner (adcrsztypvlocity_cmt__MediaType__c)
			 #Ma p to Prodcode based on ProdName to generate Product_ID ...may be not this manner....
             #ProdName = joinedDF3.select("Product_Name").rdd.flatMap(lambda x: x).collect()
             #print('Product_Name', ProdName)
             #for mindx, aMap in enumerate(Mapdata):
             #   NameSet = (aMap['SFFormat']).split("$")
             #   if (mindx==4):
             #      print('NameSet', NameSet)

             
			 
             JoinedSFColumnNames = joinedDF3.columns
             print('Joined3SFColumnNames', JoinedSFColumnNames)  
             
             #####Copying Joined1- File as | seperator (.csv)		  
             OutKey = 'Joined1-' + "07072022-Runcsv"
             Fname = 's3://'+ OutBucket + '/' + OutKey
             joinedDF1.coalesce(1).write.option("header", "true").mode("overwrite").option("sep",",").csv(Fname)
           
             ObjLocation = OutKey + '/'
             Obucket = s3_resrc.Bucket(OutBucket)
             SFFileList=[]
             for obj in Obucket.objects.filter(Prefix=ObjLocation):
                if obj.key.endswith('.csv'):
                   SFFileList.append(obj.key)
          
             print('sflist', SFFileList)
             Inputkey = SFFileList[0]
             copy_source = {'Bucket': OutBucket, 'Key': Inputkey}
             s3_client.copy_object(CopySource = copy_source, Bucket = OutBucket, Key = OutKey +  '.csv')
             s3_client.delete_object(Bucket = OutBucket, Key = Inputkey)
			 
 
             #####Copying Joined2- File as | seperator (.csv) 
             OutKey = 'Joined2-' + "07072022-Runcsv"
             Fname = 's3://'+ OutBucket + '/' + OutKey
             joinedDF2.coalesce(1).write.option("header", "true").mode("overwrite").option("sep",",").csv(Fname)
           
             ObjLocation = OutKey + '/'
             Obucket = s3_resrc.Bucket(OutBucket)
             SFFileList=[]
             for obj in Obucket.objects.filter(Prefix=ObjLocation):
                if obj.key.endswith('.csv'):
                   SFFileList.append(obj.key)
          
             print('sflist', SFFileList)
             Inputkey = SFFileList[0]
             copy_source = {'Bucket': OutBucket, 'Key': Inputkey}
             s3_client.copy_object(CopySource = copy_source, Bucket = OutBucket, Key = OutKey +  '.csv')
             s3_client.delete_object(Bucket = OutBucket, Key = Inputkey)
 
             #####Copying Joined3 File as | seperator (.csv)  
             OutKey = 'Joined3-' + "07072022-Runcsv"
             Fname = 's3://'+ OutBucket + '/' + OutKey
             joinedDF3.coalesce(1).write.option("header", "true").mode("overwrite").option("sep",",").csv(Fname)
           
             ObjLocation = OutKey + '/'
             Obucket = s3_resrc.Bucket(OutBucket)
             SFFileList=[]
             for obj in Obucket.objects.filter(Prefix=ObjLocation):
                if obj.key.endswith('.csv'):
                   SFFileList.append(obj.key)
          
             print('sflist', SFFileList)
             Inputkey = SFFileList[0]
             copy_source = {'Bucket': OutBucket, 'Key': Inputkey}
             s3_client.copy_object(CopySource = copy_source, Bucket = OutBucket, Key = OutKey +  '.csv')
             s3_client.delete_object(Bucket = OutBucket, Key = Inputkey)         
        
        #Product_ID will be added later 		
        OP1ColumnNames=['Product_Name','Home_Catalog_Location_Name','Cost_Code','Description','Created_On','Created_By_ID','Last_Modified_Date','Last_Modified_By_ID','Product_Owner_ID','Product_Type','Media_Type','Product_Status','Default_Booking_Type','Forecast_Method','Targeting_Template_Name','First_Available_Date','Tag_Name','Sponsorship_Calendar_Eligible','SF_Product_ID','SF_adSpCrSzTypId','SF_adCrSzTypId','SF_adSpSpecificationId']
        SFColumnNames= ['Product_Name','Catalog__c','Cost_Code__c','Description','CreatedDate','CreatedById','LastModifiedDate','LastModifiedById','CreatedById','vlocity_cmt__Type__c','vlocity_cmt__ObjectTypeName__c','IsActive','Default_Booking_Type__c','Forecast_Method__c','Targeting_Template__c','vlocity_cmt__FulfilmentStartDate__c','Tags__c','Sponsorship_Product__c','Id','adSpcrsztypId','adcrsztypId','adSpcSpecfyId']  
		
		#joinedDF3 to Synth file have some issue......Modification required...
        InputSFColumnNames = joinedDF3.columns  
        noOfFields = 0 
        dset = dict()
        FTargetColName = []
        for counter, aColumn in enumerate(InputSFColumnNames):
           FTargetColName.append(aColumn)
			 
        for counter, aColumn in enumerate(OP1ColumnNames):
           SourceColName = SFColumnNames[counter]
           TargetColName = OP1ColumnNames[counter]
                  
           if SourceColName in InputSFColumnNames:
              for cntr, aClmn in enumerate(InputSFColumnNames):
                 if (SourceColName == aClmn):
                    FTargetColName[cntr] = TargetColName		 
		
        print('joinedDF3-typ', type(joinedDF3))
		#dataframe = dataframe.withColumnRenamed("college", "university").withColumnRenamed("ID", "student_id")
        newjn3_df = joinedDF3.toDF(*FTargetColName)
        print('newjn3_df-typ', type(newjn3_df))
		
        for counter, aColumn in enumerate(InputSFColumnNames):
            SourceColName = aColumn
            TargetColName = FTargetColName[counter]
            tempOP1Lst = joinedDF3.select(SourceColName).rdd.flatMap(lambda x: x).collect()
            #tempOP1Lst = SFdict[SourceColName]
            dset[TargetColName] = tempOP1Lst
            if (aColumn=='CreatedById'):
               dset['Created_By_ID'] = tempOP1Lst
			
            noOfFields=noOfFields+1		  
        #print('noOfFields', noOfFields)
		
		#Insert extra OP1 fileds.......
        OP1ExtraColumnNames = ['OP1_Product_ID','OP1_Created_By_ID','OP1_Last_Modified_By_ID','OP1_Product_Owner_ID','OP1_Targeting_Template_ID','OP1_Creative_Specs_ID','OP1_Tag_ID','OP1_Revenue_Allocation_Billing_ID','OP1_Preset_Schedule_ID','Allowed_Booking_Types','Created_By','Last_Modified_By','Product_Owner','Targeting_Template_ID','Tag_ID','Creative_Specs_ID','Creative_Spec_Name','Availability_Notes','Historical_Revenue_Recognition_Method','Group_Type','Allow_User_Additions','Show_Package_On_Invoices','Show_Package_On_Export','Is_Package_Performance_Allowed','Is_Package_Target_Available','Month_Count','Week_Count','Day_Count','Allow_Overbooking','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday','Preset_Schedule_Name','Preset_Schedule_ID','Future_Revenue_Recognition_Method','Last_Available_Date','Revenue_Allocation_Billing_ID']
	
	    ###creating data frame with full col in SF
        #####Copying Synth file in mock folder in temp S3	            
        #tempOP1df = pd.DataFrame(dset)
        
        tempOP1df = newjn3_df.toPandas()
        tempOP1df['Created_By_ID'] = tempOP1df['Last_Modified_By_ID']
        
        #tempOP1dfTest = pd.DataFrame(dset)
		# add an empty columns
        #Mydataframe['Gender'] = ''
        #Mydataframe['Department'] = np.nan
        #comm now...
        for aNCol in OP1ExtraColumnNames:
           tempOP1df[aNCol] = np.nan
		
		
        ##Create combine id from SF
        tempOP1df['MComb1'] = tempOP1df.SF_Product_ID.astype(str).str.cat(tempOP1df.SF_adCrSzTypId.astype(str), sep='-')
        tempOP1df['MComb2'] = tempOP1df.MComb1.astype(str).str.cat(tempOP1df.SF_adSpSpecificationId.astype(str), sep='-')
        tempOP1df['Product_ID'] = tempOP1df.MComb2.astype(str).str.cat(tempOP1df.SF_adSpCrSzTypId.astype(str), sep='-')

		
        # Remove two columns name is 'C' and 'D' 	
        ExtraSFCols = list(set(InputSFColumnNames) - set(SFColumnNames))
        ExtraSFCols.append('MComb1')
        ExtraSFCols.append('MComb2')	
        print('ExtraSFCols', ExtraSFCols)
		
        #ExtraSFCols=ExtraSFCols.append('name1')
        #ExtraSFCols=ExtraSFCols.append('name2')		
        #ExtraSFCols=ExtraSFCols.append('name3')
        #ExtraSFCols=ExtraSFCols.append('name4')		
        #ExtraSFCols=ExtraSFCols.append('namedmmy')		

        extraset = ['OP1_Product_ID','OP1_Created_By_ID','OP1_Last_Modified_By_ID','OP1_Product_Owner_ID','OP1_Targeting_Template_ID','OP1_Creative_Specs_ID','OP1_Tag_ID','OP1_Revenue_Allocation_Billing_ID','OP1_Preset_Schedule_ID','Cost_Code','Home_Catalog_Location_Name','SF_Product_ID','SF_adSpSpecificationId','SF_adSpCrSzTypId','SF_adCrSzTypId'] 
        FinalColumnNames = tempOP1df.columns
        FinalOP1Cols = OP1ColumnNames + OP1ExtraColumnNames
        OnlySFCols = list(set(FinalColumnNames)-set(FinalOP1Cols+extraset))
        #FinalSynthCols = FinalOP1Cols + OnlySFCols
        ProdSeqHeraderList = ['Product_ID','Product_Name','Description','Created_On','Created_By','Created_By_ID','Last_Modified_Date','Last_Modified_By','Last_Modified_By_ID','Product_Owner','Product_Owner_ID','Product_Type','Media_Type','Product_Status','Allowed_Booking_Types','Default_Booking_Type','Forecast_Method','Targeting_Template_ID','Targeting_Template_Name','Creative_Specs_ID','Creative_Spec_Name','First_Available_Date','Last_Available_Date','Availability_Notes','Tag_ID','Tag_Name','Historical_Revenue_Recognition_Method','Future_Revenue_Recognition_Method','Group_Type','Allow_User_Additions','Show_Package_On_Invoices','Show_Package_On_Export','Is_Package_Performance_Allowed','Is_Package_Target_Available','Revenue_Allocation_Billing_ID','Preset_Schedule_ID','Preset_Schedule_Name','Sponsorship_Calendar_Eligible','Month_Count','Week_Count','Day_Count','Allow_Overbooking','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
        aminusb = list(set(ProdSeqHeraderList)-set(FinalOP1Cols))
        bminusa = list(set(FinalOP1Cols)-set(ProdSeqHeraderList))
        print('aminusb', aminusb)
        print('bminusa', bminusa)
        OnlySFColsT = list(set(OnlySFCols)-set(['MComb2','namedmmy','name3','MComb1','name4','name1','name2']))
        FinalSynthCols = ProdSeqHeraderList + extraset + OnlySFColsT
		
		
        #drop set 'adcrsztypCreatedDate','adSpcSpecfyCreatedDate','adcrsztypCreatedById','adSpcSpecfyCreatedById','adSpcrsztypCreatedById','adSpcrsztypCreatedDate'
		#col sort as per FinalSynthCols
        tempOP1df_1 = tempOP1df[FinalSynthCols] 
        ExtraSFCols11 = list(set(ExtraSFCols) - set(['MComb2','namedmmy','name3','MComb1','name4','name1','name2']))
        DeleteList = ['aws_request_id','LastReferencedDate','vlocity_cmt__IsNotAssetizable__c','RecordTypeId','SystemModstamp','vlocity_cmt__GlobalGroupKey__c','Product_ID','filename','function_name','processed_ts','SF_Product_ID','SF_adSpSpecificationId','SF_adSpCrSzTypId','SF_adCrSzTypId','adcrsztypaws_request_id','adcrsztypLastModifiedById','adSpcSpecfyvlocity_cmt__PublisherDayPart__c','adSpcSpecfyvlocity_cmt__StartTime__c','adcrsztypvlocity_cmt__Width__c','adSpcrsztypCreatedById','adSpcSpecfyCreatedById','adcrsztypIsDeleted','adSpcSpecfyvlocity_cmt__MediaContentTitleId__c','adSpcSpecfyfunction_version','adSpcrsztypLastModifiedDate','adSpcSpecfyvlocity_cmt__EndTime__c','adcrsztypSystemModstamp','adSpcrsztypCreatedDate','adcrsztypvlocity_cmt__MediaType__c','adSpcrsztypfunction_version','adSpcSpecfyvlocity_cmt__IsActive__c','adSpcSpecfyappflow_id','adSpcSpecfyvlocity_cmt__Product2Id__c','adSpcrsztypaws_request_id','adSpcSpecfyIsDeleted','adSpcSpecfyvlocity_cmt__IsLiveBroadcast__c','adcrsztypvlocity_cmt__UnitOfMeasure__c','adSpcSpecfyOwnerId','adSpcSpecfyvlocity_cmt__EndDateTime__c','adSpcrsztypIsDeleted','adcrsztypvlocity_cmt__RunTime__c','adSpcrsztypvlocity_cmt__CoreId__c','adSpcrsztypfilename','adSpcSpecfySystemModstamp','adSpcSpecfyfilename','adcrsztypfunction_version','adSpcSpecfyvlocity_cmt__EndWeekDay__c','adSpcSpecfyvlocity_cmt__MediaPropertyId__c','adSpcrsztypvlocity_cmt__AdSpaceSpecificationId__c','adSpcSpecfyvlocity_cmt__AudienceSizeRating__c','adSpcSpecfyvlocity_cmt__ProgramRunType__c','adcrsztypfilename','adSpcSpecfyvlocity_cmt__AdServerId__c','adSpcSpecfyvlocity_cmt__CoreId__c','adSpcSpecfyvlocity_cmt__AdServerAdSpaceIdentifier__c','adSpcrsztypfunction_name','adcrsztypprocessed_ts','adcrsztypName','adSpcSpecfyCreatedDate','adSpcrsztypName','adSpcrsztypLastModifiedById','adSpcSpecfyName','adSpcrsztypappflow_id','adSpcSpecfyRecordTypeId','adSpcSpecfyDevice__c','adSpcSpecfyvlocity_cmt__StartDateTime__c','adSpcSpecfyfunction_name','adSpcSpecfyLastModifiedDate','adSpcSpecfyvlocity_cmt__AdSpaceType__c','adSpcSpecfyvlocity_cmt__StartWeekDay__c','adcrsztypvlocity_cmt__Height__c','adcrsztypOwnerId','adSpcrsztypvlocity_cmt__AdCreativeSizeTypeId__c','adcrsztypvlocity_cmt__CoreId__c','adcrsztypfunction_name','adSpcSpecfyaws_request_id','adSpcrsztypSystemModstamp','adcrsztypLastModifiedDate','adSpcSpecfyLastModifiedById','adSpcSpecfyprocessed_ts','adcrsztypCreatedDate','adcrsztypappflow_id','adSpcrsztypprocessed_ts','adcrsztypCreatedById','appflow_id']
        ExtraSFCols1 = ExtraSFCols11 + DeleteList
        OP1tempOP1df = tempOP1df_1
        OP1tempOP1df=OP1tempOP1df.drop(DeleteList, axis = 1)
        print('ExtraSFCols1', ExtraSFCols1)
        #ExtraListF=['OP1_Product_ID','OP1_Created_By_ID','OP1_Last_Modified_By_ID','OP1_Product_Owner_ID','OP1_Targeting_Template_ID','OP1_Creative_Specs_ID','OP1_Tag_ID','OP1_Revenue_Allocation_Billing_ID','OP1_Preset_Schedule_ID','Agency_Commission_applicable__c','CanUseQuantitySchedule','CanUseRevenueSchedule','Cost_Code','DisplayUrl','Embedded_Targeting__c','ExternalDataSourceId','ExternalId','Family','function_version','Home_Catalog_Location_Name','IsArchived','IsDeleted','LastViewedDate','Loc1__c','Name','NumberOfQuantityInstallments','NumberOfRevenueInstallments','Premium_Product__c','Pricing_Method__c','Product_Category__c','Product_Type__c','ProductCode','Production_System__c','Promo_applicable__c','QuantityInstallmentPeriod','QuantityScheduleType','QuantityUnitOfMeasure','RevenueInstallmentPeriod','RevenueScheduleType','Rich_Media_Involved__c','StockKeepingUnit','SWM_Automatic_Guaranteed__c','SWM_Direct_Orders__c','SWM_Programmatic_Guaranteed_Orders__c','Targeting_Category__c','vlocity_cmt__ApprovedBy__c','vlocity_cmt__ApprovedOn__c','vlocity_cmt__AttributeDefaultValues__c','vlocity_cmt__AttributeRules__c','vlocity_cmt__AttributesMarkupConfig__c','vlocity_cmt__CategoryData__c','vlocity_cmt__ChangeDetectorImplementation__c','vlocity_cmt__ClassId__c','vlocity_cmt__DefaultImageId__c','vlocity_cmt__EffectiveDate__c','vlocity_cmt__EndDate__c','vlocity_cmt__EndOfLifeDate__c','vlocity_cmt__GlobalKey__c','vlocity_cmt__HeaderData__c','vlocity_cmt__HelpText__c','vlocity_cmt__ImageId__c','vlocity_cmt__IsConfigurable__c','vlocity_cmt__IsCustomerSubscription__c','vlocity_cmt__IsLocationDependent__c','vlocity_cmt__IsOrderable__c','vlocity_cmt__JeopardySafetyInterval__c','vlocity_cmt__JeopardySafetyIntervalUnit__c','vlocity_cmt__LifecycleStatus__c','vlocity_cmt__MainFeatureQuantity__c','vlocity_cmt__MainFeatureQuantityUom__c','vlocity_cmt__MainFeatureTermInDays__c','vlocity_cmt__Modification__c','vlocity_cmt__ObjectTypeId__c','vlocity_cmt__ParentClassCode__c','vlocity_cmt__ParentClassId__c','vlocity_cmt__ProductSpecId__c','vlocity_cmt__ProductSpecName__c','vlocity_cmt__ProductTemplateProductId__c','vlocity_cmt__Scope__c','vlocity_cmt__SellingEndDate__c','vlocity_cmt__SellingStartDate__c','vlocity_cmt__SpecificationSubType__c','vlocity_cmt__SpecificationType__c','vlocity_cmt__StandardPremium__c','vlocity_cmt__Status__c','vlocity_cmt__SubType__c','vlocity_cmt__TimeToLive__c','vlocity_cmt__TrackAsAgreement__c','vlocity_cmt__VendorAccountId__c','vlocity_cmt__VersionEndDateTime__c','vlocity_cmt__VersionLabel__c','vlocity_cmt__VersionStartDateTime__c']
        ExtraListF = ['OP1_Product_ID','OP1_Created_By_ID','OP1_Last_Modified_By_ID']
        FinalSynthColsF = ProdSeqHeraderList + ExtraListF
        OP1tempOP1df = OP1tempOP1df[FinalSynthColsF] 
        OP1tempOP1df=OP1tempOP1df.replace(np.nan, '', regex=True)
		
        #aTable = 'Products'		
        ###Synthetic file write.... version 1.0
        OutKey = 'version 1.0/Synth-Products'+"_07072022" +  ".csv"
        out_bytes = io.BytesIO()
        OP1tempOP1df.to_csv(out_bytes, index=False)
        out_bytes.seek(0)
        s3_client.put_object(Bucket=OutBucket, Key=OutKey, Body=out_bytes.read())
        
        
        #OutKey = 'version 1.0/Synth-Products'+ "_07072022"
        #Fname = 's3://'+ OutBucket + '/' + OutKey
        #new_df.coalesce(1).write.option("header", "true").mode("overwrite").option("sep",",").csv(Fname)
        #ObjLocation = OutKey + '/'
        #Obucket = s3_resrc.Bucket(OutBucket)
        #SFFileList=[]
        #for obj in Obucket.objects.filter(Prefix=ObjLocation):
        #    if obj.key.endswith('.csv'):
        #         SFFileList.append(obj.key)
          
        #print('sflist', SFFileList)
        #Inputkey = SFFileList[0]
        #copy_source = {'Bucket': OutBucket, 'Key': Inputkey}
        #s3_client.copy_object(CopySource = copy_source, Bucket = OutBucket, Key = OutKey +  '.csv')
        #s3_client.delete_object(Bucket = OutBucket, Key = Inputkey)      
        
           
        #####Copying Synth file in OP1 swm-adsales-ordermanagement/operative-synthetic/development/		   
        #for OP1 swm-adsales-ordermanagement/operative-synthetic/development/
        OutKey = 'version 1.0/Synth-Products'+ "_07072022.csv"
        InputKey = OutKey
        old_obj = s3_resrc.Object(OutBucket, InputKey)
        # create a reference for destination image
        new_obj = dest_s3_r.Object('swm-adsales-ordermanagement', 'operative-synthetic/development/'+InputKey)
        # upload the image to destination S3 object
        new_obj.put(Body=old_obj.get()['Body'].read())
        print('Copied a file in 7WestS3 swm-adsales-ordermanagement :: ', 'operative-synthetic/development/'+InputKey)       
   
   
   
   
		####Only OP1 file write..... dropping extra cols from sf  
        #OutKey = 'OP1-'+aTable + "-07072022" +  ".csv"
        #out_bytes = io.BytesIO()
        #OP1tempOP1df.to_csv(out_bytes, index=False)
        #out_bytes.seek(0)
        ##print(f"Writing to s3://{OutBucket}/{OutKey}")
        #s3_client.put_object(Bucket=OutBucket, Key=OutKey, Body=out_bytes.read())
           
        ######Copying OP1 file in OP1 swm-adsales-ordermanagement/operative-synthetic/development/		   
        ##for OP1 swm-adsales-ordermanagement/operative-synthetic/development/
        #InputKey = OutKey
        #old_obj = s3_resrc.Object(OutBucket, InputKey)
        ## create a reference for destination image
        #new_obj = dest_s3_r.Object('swm-adsales-ordermanagement', 'operative-synthetic/development/'+InputKey)
        ## upload the image to destination S3 object
        #new_obj.put(Body=old_obj.get()['Body'].read())
        #print('Copied a file in 7WestS3 swm-adsales-ordermanagement :: ', 'operative-synthetic/development/'+InputKey)   


        ######Creative_Specs_01262022....pending
        CreativeSpecMap = ['Creative_Spec_ID','Creative_Spec_Name','Media_Type','Description','Primary_Measurement_Unit','Height','Width','Expanded_Height','Expanded_Width','Allowed_Expanded_Directions','HTML_Max_Code_Size','Max_Initial_Load_File_Size(k)','Max File Size Flash / Rich Media(k)','Max File Size GIF/JPG(k)','Max_File_Size_Notes','Max Anim. Time(s)','Max_FPS','Max_Animation_Loops','Ad_Type','Broadband_Min_Time','Broadband_Max_Time','Broadband_Format','Broadband_Preformatted','Broadband_Clickable','Broadband_Companion_Ads','Alt_Text_Max_Chars','Third_Party_Tags_Accepted','Third_Party_Tracking_Accepted','Sound_Allowed','Max_Creatives_Per_Campaign_Image_Rotation','Uses_IFRAME_Javascript','Approved_Rich_Media_Vendors','Delivery_FlashRichMedia_No_Of_Days_Before_Launch','Delivery_GIFJPG_No_Of_Days_Before_Launch','Notes','No_of_Columns','Mechanical_Specifications','Submission_Guidelines','Production_Notes','Bleed_Allowed'] 
        ExtraColCrSpec = ['Product_ID','Ad_Slot_ID','Payment_Term_ID','Payment_Term_Name']
        ExtraValCrSpec = ['','','','']


        #######Product_Ad_Slot_Map
        AdSlotMap = ['Product_ID','Product_Name','adSpcSpecfyName'] 
        ExtraColAd = ['OP1_Product_ID','OP1_Ad_Slot_ID','OP1_Payment_Term_ID','Payment_Term_Name']
        ExtraValAd = ['','','','']		
        AdSlotOP1tempOP1df = tempOP1df[AdSlotMap]
        for ijk, aNCol in enumerate(ExtraColAd):
           AdSlotOP1tempOP1df[aNCol] = ExtraValAd[ijk]
        AdSlotOP1tempOP1df.rename(columns = {'adSpcSpecfyName':'Ad_Slot_Name'}, inplace = True) 	
		
		###Only Synth file write.....  Product_Ad_Slot_Map  rename Ad_Slot_Name
        aTable = 'Product_Ad_Slot_Map'
        OutKey = 'version 1.0/Synth-'+aTable + "_07072022" +  ".csv"
        out_bytes = io.BytesIO()
        AdSlotOP1tempOP1df.to_csv(out_bytes, index=False)
        out_bytes.seek(0)
        #print(f"Writing to s3://{OutBucket}/{OutKey}")
        s3_client.put_object(Bucket=OutBucket, Key=OutKey, Body=out_bytes.read())
           
        #####Copying OP1 file in OP1 swm-adsales-ordermanagement/operative-synthetic/development/		   
        #for OP1 swm-adsales-ordermanagement/operative-synthetic/development/
        InputKey = OutKey
        old_obj = s3_resrc.Object(OutBucket, InputKey)
        # create a reference for destination image
        new_obj = dest_s3_r.Object('swm-adsales-ordermanagement', 'operative-synthetic/development/'+InputKey)
        # upload the image to destination S3 object
        new_obj.put(Body=old_obj.get()['Body'].read())
        print('Copied a file in 7WestS3 swm-adsales-ordermanagement :: ', 'operative-synthetic/development/'+InputKey)   


        ######  product_custom_field
        CstmFldMap = ['Product_ID','Product_Name','Cost_Code','Created_By_ID','Created_On','Last_Modified_By_ID','Last_Modified_Date'] 
        ExtraCol = ['OP1_Product_ID','Custom_Field_ID','Custom_Field_Name','Custom_Field_Type_ID','Custom_Field_Type','Custom_Field_Status_ID','Custom_Field_Status','Is_Required','Is_Read_Only','Is_Masked','API_Name','Selection_Type','Dropdown_Type','Created_By','Last_Modified_By']
        ExtraVal = ['','32','Cost Code','3000','Text','1','active','0','0','0','cost_code','','','','']
        CstmFldOP1tempOP1df = OP1tempOP1df[CstmFldMap]
        for ijk, aNCol in enumerate(ExtraCol):
           CstmFldOP1tempOP1df[aNCol] = ExtraVal[ijk]
        CstmFldOP1tempOP1df.rename(columns = {'Cost_Code':'Custom_Field_Value'}, inplace = True) 
        CstmFldOP1tempOP1df.rename(columns = {'Last_Modified_Date':'Last_Modified_On'}, inplace = True) 

		###Only OP1 file write.....  product_custom_field 
        aTable = 'Product_Custom_Field'
        OutKey = 'version 1.0/Synth-'+aTable + "_07072022" +  ".csv"
        out_bytes = io.BytesIO()
        CstmFldOP1tempOP1df.to_csv(out_bytes, index=False)
        out_bytes.seek(0)
        #print(f"Writing to s3://{OutBucket}/{OutKey}")
        s3_client.put_object(Bucket=OutBucket, Key=OutKey, Body=out_bytes.read())
           
        #####Copying OP1 file in OP1 swm-adsales-ordermanagement/operative-synthetic/development/		   
        #for OP1 swm-adsales-ordermanagement/operative-synthetic/development/
        InputKey = OutKey
        old_obj = s3_resrc.Object(OutBucket, InputKey)
        # create a reference for destination image
        new_obj = dest_s3_r.Object('swm-adsales-ordermanagement', 'operative-synthetic/development/'+InputKey)
        # upload the image to destination S3 object
        new_obj.put(Body=old_obj.get()['Body'].read())
        print('Copied a file in 7WestS3 swm-adsales-ordermanagement :: ', 'operative-synthetic/development/'+InputKey)  


        ###Product_Location_Map        
        PrdLocMap = ['Product_ID','Product_Name','Home_Catalog_Location_Name'] 
        ExtraColloc = ['OP1_Product_ID','Home_Catalog_Location_ID','Mapped_Catalog_Location_ID','Mapped_Catalog_Location_Name']
        ExtraValloc = ['','','','']		
        PrdLocOP1tempOP1df = tempOP1df[PrdLocMap]
        for ijk, aNCol in enumerate(ExtraColloc):
           PrdLocOP1tempOP1df[aNCol] = ExtraValloc[ijk]		
        #PrdLocOP1tempOP1df.rename(columns = {'Catalog__c':'Home_Catalog_Location_Name'}, inplace = True) 	
		
		###Only Synth file write.....  Product_Location_Map
        aTable = 'Product_Location_Map'
        OutKey = 'version 1.0/Synth-'+aTable + "_07072022" +  ".csv"
        out_bytes = io.BytesIO()
        PrdLocOP1tempOP1df.to_csv(out_bytes, index=False)
        out_bytes.seek(0)
        #print(f"Writing to s3://{OutBucket}/{OutKey}")
        s3_client.put_object(Bucket=OutBucket, Key=OutKey, Body=out_bytes.read())
           
        #####Copying OP1 file in OP1 swm-adsales-ordermanagement/operative-synthetic/development/		   
        #for OP1 swm-adsales-ordermanagement/operative-synthetic/development/
        InputKey = OutKey
        old_obj = s3_resrc.Object(OutBucket, InputKey)
        # create a reference for destination image
        new_obj = dest_s3_r.Object('swm-adsales-ordermanagement', 'operative-synthetic/development/'+InputKey)
        # upload the image to destination S3 object
        new_obj.put(Body=old_obj.get()['Body'].read())
        print('Copied a file in 7WestS3 swm-adsales-ordermanagement :: ', 'operative-synthetic/development/'+InputKey)   


              
        ####Writting original SF Product joined table as $ separator at S3 temp folder
        OutKey = 'SF-ProductJoined'
        Fname = 's3://'+ OutBucket + '/' + OutKey
        joinedDF3.coalesce(1).write.option("header", "true").mode("overwrite").option("sep","$").csv(Fname)
           
        ObjLocation = OutKey + '/'
        Obucket = s3_resrc.Bucket(OutBucket)
        SFFileList=[]
        #for key in bucket.objects.all():
        for obj in Obucket.objects.filter(Prefix=ObjLocation):
             if obj.key.endswith('.csv'):
               SFFileList.append(obj.key)
          
        print('sflist', SFFileList)
        Inputkey = SFFileList[0]
        copy_source = {'Bucket': OutBucket, 'Key': Inputkey}
        s3_client.copy_object(CopySource = copy_source, Bucket = OutBucket, Key = OutKey +  '.csv')
        s3_client.delete_object(Bucket = OutBucket, Key = Inputkey)
          
        # create a reference to source image swm-adsales-ordermanagement/salesforce/development/
        InputKey = OutKey +  '.csv'
        old_obj = s3_resrc.Object(OutBucket, InputKey)
        # create a reference for destination image
        new_obj = dest_s3_r.Object('swm-adsales-ordermanagement', 'salesforce/development/'+InputKey)
        # upload the image to destination S3 object
        new_obj.put(Body=old_obj.get()['Body'].read())
        print('Copied a file in 7WestS3 swm-adsales-ordermanagement :: ', 'salesforce/development/'+InputKey)
  
        ####Writting original SF Product table as $ separator at S3 temp folder
        OutKey = 'SF-Product'
        Fname = 's3://'+ OutBucket + '/' + OutKey
        Product2.coalesce(1).write.option("header", "true").mode("overwrite").option("sep",",").csv(Fname)
           
        ObjLocation = OutKey + '/'
        Obucket = s3_resrc.Bucket(OutBucket)
        SFFileList=[]
        #for key in bucket.objects.all():
        for obj in Obucket.objects.filter(Prefix=ObjLocation):
             if obj.key.endswith('.csv'):
               SFFileList.append(obj.key)
          
        print('sflist', SFFileList)
        Inputkey = SFFileList[0]
        copy_source = {'Bucket': OutBucket, 'Key': Inputkey}
        s3_client.copy_object(CopySource = copy_source, Bucket = OutBucket, Key = OutKey +  '.csv')
        s3_client.delete_object(Bucket = OutBucket, Key = Inputkey)
          
        # create a reference to source image swm-adsales-ordermanagement/salesforce/development/
        InputKey = OutKey +  '.csv'
        old_obj = s3_resrc.Object(OutBucket, InputKey)
        # create a reference for destination image
        new_obj = dest_s3_r.Object('swm-adsales-ordermanagement', 'salesforce/development/'+InputKey)
        # upload the image to destination S3 object
        new_obj.put(Body=old_obj.get()['Body'].read())
        print('Copied a file in 7WestS3 swm-adsales-ordermanagement :: ', 'salesforce/development/'+InputKey)

	    #SFUnqTableNames = ['Ad-Space-Specification  adSpcSpecfy', 'Ad-Space-Creative-Size-Type   adSpcrsztyp','Ad-Creative-Size-Type  adcrsztyp']
        ####Writting original SF Product table as $ separator at S3 temp folder
        OutKey = 'SF-Ad-Space-Specification'
        Fname = 's3://'+ OutBucket + '/' + OutKey
        adSpcSpecfy.coalesce(1).write.option("header", "true").mode("overwrite").option("sep",",").csv(Fname)
           
        ObjLocation = OutKey + '/'
        Obucket = s3_resrc.Bucket(OutBucket)
        SFFileList=[]
        #for key in bucket.objects.all():
        for obj in Obucket.objects.filter(Prefix=ObjLocation):
             if obj.key.endswith('.csv'):
               SFFileList.append(obj.key)
          
        print('sflist', SFFileList)
        Inputkey = SFFileList[0]
        copy_source = {'Bucket': OutBucket, 'Key': Inputkey}
        s3_client.copy_object(CopySource = copy_source, Bucket = OutBucket, Key = OutKey +  '.csv')
        s3_client.delete_object(Bucket = OutBucket, Key = Inputkey)
          
        # create a reference to source image swm-adsales-ordermanagement/salesforce/development/
        InputKey = OutKey +  '.csv'
        old_obj = s3_resrc.Object(OutBucket, InputKey)
        # create a reference for destination image
        new_obj = dest_s3_r.Object('swm-adsales-ordermanagement', 'salesforce/development/'+InputKey)
        # upload the image to destination S3 object
        new_obj.put(Body=old_obj.get()['Body'].read())
        print('Copied a file in 7WestS3 swm-adsales-ordermanagement :: ', 'salesforce/development/'+InputKey)


	    #SFUnqTableNames = ['Ad-Space-Creative-Size-Type   adSpcrsztyp','Ad-Creative-Size-Type  adcrsztyp']
        ####Writting original SF Product table as $ separator at S3 temp folder
        OutKey = 'SF-Ad-Space-Creative-Size-Type'
        Fname = 's3://'+ OutBucket + '/' + OutKey
        adSpcrsztyp.coalesce(1).write.option("header", "true").mode("overwrite").option("sep",",").csv(Fname)
           
        ObjLocation = OutKey + '/'
        Obucket = s3_resrc.Bucket(OutBucket)
        SFFileList=[]
        #for key in bucket.objects.all():
        for obj in Obucket.objects.filter(Prefix=ObjLocation):
             if obj.key.endswith('.csv'):
               SFFileList.append(obj.key)
          
        print('sflist', SFFileList)
        Inputkey = SFFileList[0]
        copy_source = {'Bucket': OutBucket, 'Key': Inputkey}
        s3_client.copy_object(CopySource = copy_source, Bucket = OutBucket, Key = OutKey +  '.csv')
        s3_client.delete_object(Bucket = OutBucket, Key = Inputkey)
          
        # create a reference to source image swm-adsales-ordermanagement/salesforce/development/
        InputKey = OutKey +  '.csv'
        old_obj = s3_resrc.Object(OutBucket, InputKey)
        # create a reference for destination image
        new_obj = dest_s3_r.Object('swm-adsales-ordermanagement', 'salesforce/development/'+InputKey)
        # upload the image to destination S3 object
        new_obj.put(Body=old_obj.get()['Body'].read())
        print('Copied a file in 7WestS3 swm-adsales-ordermanagement :: ', 'salesforce/development/'+InputKey)


	    #SFUnqTableNames = ['Ad-Creative-Size-Type  adcrsztyp']
        ####Writting original SF Product table as $ separator at S3 temp folder
        OutKey = 'SF-Ad-Creative-Size-Type'
        Fname = 's3://'+ OutBucket + '/' + OutKey
        adcrsztyp.coalesce(1).write.option("header", "true").mode("overwrite").option("sep",",").csv(Fname)
           
        ObjLocation = OutKey + '/'
        Obucket = s3_resrc.Bucket(OutBucket)
        SFFileList=[]
        #for key in bucket.objects.all():
        for obj in Obucket.objects.filter(Prefix=ObjLocation):
             if obj.key.endswith('.csv'):
               SFFileList.append(obj.key)
          
        print('sflist', SFFileList)
        Inputkey = SFFileList[0]
        copy_source = {'Bucket': OutBucket, 'Key': Inputkey}
        s3_client.copy_object(CopySource = copy_source, Bucket = OutBucket, Key = OutKey +  '.csv')
        s3_client.delete_object(Bucket = OutBucket, Key = Inputkey)
          
        # create a reference to source image swm-adsales-ordermanagement/salesforce/development/
        InputKey = OutKey +  '.csv'
        old_obj = s3_resrc.Object(OutBucket, InputKey)
        # create a reference for destination image
        new_obj = dest_s3_r.Object('swm-adsales-ordermanagement', 'salesforce/development/'+InputKey)
        # upload the image to destination S3 object
        new_obj.put(Body=old_obj.get()['Body'].read())
        print('Copied a file in 7WestS3 swm-adsales-ordermanagement :: ', 'salesforce/development/'+InputKey)

  
    except Exception as e:
         print("Error occurred in main method - %s" % e)
        
    
        # If you don't throw an exception then the job succeeds
         raise e
    finally:
        end_timestmp = datetime.datetime.utcnow()
        if (ProcessGlueCode == 1): 
           print('Glue Job ends successfully for Product Transformation')
        
