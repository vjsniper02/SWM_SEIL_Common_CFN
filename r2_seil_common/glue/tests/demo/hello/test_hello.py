import pytest
import json
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from tests import util as U


@pytest.fixture()
def mappings_cust():
    return json.loads(r"""
{
   "tobj":"customer",
   "sobj":"raw_customer",
   "stype":"tbl",
   "ttype":"tbl",
   "mappings":[
      {
         "tgt":{
            "name":"cust_id",
            "type":"string"
         },
         "map_type":"sql",
         "map_rule":"concat('SWM_', raw_id)",
         "src":{
            "name":"raw_id",
            "type":"string",
            "vals":[
               {
                  "type":"re",
                  "msg":"raw_id must be digit",
                  "rule":"^\\d+$"
               }
            ]
         }
      },
      {
         "tgt":{
            "name":"cust_age",
            "type":"int"
         },
         "src":{
            "name":"raw_age",
            "type":"string",
            "vals":[
               {
                  "type":"sql",
                  "msg":"raw_age must be between 18 and 65",
                  "rule":"cast(raw_age as int) between 18 and 65"
               }
            ]
         }
      }
   ]
}""")


@pytest.fixture()
def raw_customer(glueContext: GlueContext) -> DynamicFrame:
    spark = glueContext.spark_session
    df = spark.createDataFrame([("100", "b"), ("c", "d")], ["c1", "c2"])
    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
    return dyf
    

def test_hello():
   assert 1 == 1
   