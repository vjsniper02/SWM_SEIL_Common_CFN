# This is demo for schema and transformation.
#
#
import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F
import json
import boto3
import botocore
from boto3.dynamodb.conditions import Key
from awsglue.transforms import Relationalize


def query_mapping(sobj, tobj, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://dynamodb.ap-southeast-2.amazonaws.com")
    table = dynamodb.Table('swmi_dev_obj_mapping')
    try:
        response = table.get_item(Key={'sobj': sobj, 'tobj': tobj})
    except botocore.exceptions.ClientError as e:
        logger.error("failed to query_map. Error: %s", e.response['Error']['Message'])
        raise e
    else:
        logger.info("query_mapping response[Item] = " + json.dumps(response))
        return response['Item']

def load_source(obj_map):
    raw = glueContext.create_dynamic_frame_from_options(obj_map["sconn"]["type"], connection_options=obj_map["sconn"].get("options"), \
        format=obj_map["sconn"].get("format"), format_options=obj_map["sconn"].get("format_options", {}), transformation_ctx = obj_map["sobj"])

    if obj_map["sconn"]["format"] == "json":
        dfc = Relationalize.apply(frame = raw, staging_path = "s3://yh-swmi-test/temp", name = "dfc_root_table_name", transformation_ctx = "dfc")
        df = dfc.select("dfc_root_table_name").toDF()
        for c in df.columns:
            df = df.withColumnRenamed(c, c.replace(".", "_"))
        flatten_raw = DynamicFrame.fromDF(df, glueContext, "faltten_raw")
    else:
        flatten_raw = raw
        
    return flatten_raw


def validate_rule(att, v):
    if v['type'] == 're': 
        esr = re.sub(r'\\', r'\\\\', v['rule'])
        cond = f"{att} rlike '{esr}'" 
    elif v['type'] == 'sql': 
        cond = v['rule']
    else: 
        None

    if cond is None:
        return None
    else:
        return f"(case when {cond} then NULL else '{v.get('msg',att)}' end)"
        
def validate_att(att, vals):
    if vals != []:
        val_res = ','.join([validate_rule(att, v) for v in vals])
        return f"array({val_res})"
    else:
        return None
    
def get_mappings(obj_map, side='src'):
    return [m[side] for m in obj_map['mappings'] if side in m ]


def validate(obj, obj_map, side='src'):
    mappings = get_mappings(obj_map, side='src')
    obj_df = obj.toDF()
    cols = obj_df.columns
    obj_df = obj_df.withColumn('val_msg', F.array())
    for att in cols:
        vals = next((m.get('vals') for m in mappings if m['name'] == att), None)
        if vals is not None:
            r = validate_att(att,vals)
            expr = f"array_union(val_msg, {r})"
            obj_df = obj_df.withColumn('val_msg', F.expr(expr))
    obj_df = obj_df.withColumn('val_msg', F.expr("nullif(array_join(val_msg,'|'),'')") )
    return DynamicFrame.fromDF(obj_df, glueContext, "val_raw_cust")


def transform(val_res, obj_map):
    val_df = val_res.toDF()
    if 'val_msg' in val_df.columns:
        new_df = val_df.filter("val_msg is NULL")
    else:
        new_df = val_df
    new_prefix = '__newcol__'
    for m in obj_map['mappings']:
        if (m.get('tgt') is not None) and (m['tgt'].get('name') is not None):
            c = m['tgt']['name']
            map_type = m.get('map_type')
            map_rule = m.get('map_rule')
            new_dtype = m['tgt']['type']
            new_col = f"{new_prefix}{c}"
            if (map_type == 'sql') and (map_rule is not None):
                new_df = new_df.withColumn(new_col, F.expr(map_rule).cast(new_dtype))
            else:
                src_col = m['src']['name']
                new_df = new_df.withColumn(new_col, F.col(src_col).cast(new_dtype))
    col_sel = [c for c in new_df.columns if c.startswith(new_prefix)]
    new_df = new_df.select(col_sel)
    for c in new_df.columns:
        new_df = new_df.withColumnRenamed(c, c[len(new_prefix):])
    return DynamicFrame.fromDF(new_df, glueContext, obj_map['tobj'])

def write_ex_export(obj_map, val_res):
    dyf_ex = val_res.filter(f=lambda x: x["val_msg"] is not None, transformation_ctx="filtered_val_res", info="Filter val_res", \
        stageThreshold=0, totalThreshold=0)
    glueContext.write_dynamic_frame.from_options(
        frame = dyf_ex,
        connection_type = obj_map["exconn"]["type"], 
        connection_options = obj_map["exconn"].get("options"), 
        format = obj_map["exconn"].get("format"), 
        format_options=obj_map["exconn"].get("format_options"), 
        transformation_ctx = obj_map["tobj"]+"_ex")

def write_export(obj_map, tran_res):
    glueContext.write_dynamic_frame.from_options(
        frame = tran_res,
        connection_type = obj_map["tconn"]["type"], 
        connection_options = obj_map["tconn"].get("options"), 
        format = obj_map["tconn"].get("format"), 
        format_options=obj_map["tconn"].get("format_options"), 
        transformation_ctx = obj_map["tobj"])

def main():
    global glueContext 
    glueContext = GlueContext(SparkContext.getOrCreate())
    global logger 
    logger = glueContext.get_logger()
    global spark 
    spark = glueContext.spark_session

    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    sobj = "raw_player"
    tobj = "player"
    # sobj = "raw_customer"
    # tobj = "customer"

    obj_map = query_mapping(sobj, tobj)
    print(f"obj_map is {obj_map}")

    sdyf = load_source(obj_map)
    if not sdyf.toDF().head(1):
        logger.info("No source data found")
        return

    sdyf.toDF().show(truncate=False)
    val_res = validate(sdyf, obj_map)

    # val_res.toDF().show(truncate=False)
    write_ex_export(obj_map, val_res)
    
    tran_res = transform(val_res, obj_map)
    # tran_res.toDF().show(truncate=False)
    write_export(obj_map, tran_res)

    logger.info("successfully with %s->%s" % (sobj, tobj))

    job.commit()

if __name__ == '__main__':
    main()
