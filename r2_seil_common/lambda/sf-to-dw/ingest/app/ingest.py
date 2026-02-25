""" This function sends the landing file to curated bucket for datawarehouse usage.

It is triggered when a Salesforce file lands on the landing S3 bucket. Then some lineage columns are appended to the data.
The final parquet files are created in the curated bucket.

For example: in dev environment,
s3://swmi-dev-sf-to-dw-landing/in/<env>-Salesforce-Account/<uuid>/<filename>  --- Location of incoming Salesforce Account objects via AppFlow
s3://swmi-dev-sf-to-dw-landing/processed/Salesforce-Account/<year>/<month>/<day>/<hour>/<filename>  --- If a file has proper schema and is processed, it is moved here.
s3://swmi-dev-sf-to-dw-landing/error/Salesforce-Account/<year>/<month>/<day>/<hour>/<filename>  --- If a file has invalid schema, it is moved here.

s3://swmi-dev-sf-to-dw-curated/Salesforce-Account/<year>/<month>/<day>/<hour>/curated_<filename>  --- If a file has valid schema, it is appended with lineage columns and saved here.

"""
import json
import logging
import os
from datetime import datetime as dt
import boto3
from aws_xray_sdk.core import patch_all
import pyarrow.parquet as pq
import pyarrow as pa
from functools import reduce
import re


logger = logging.getLogger()
logger.setLevel(logging.INFO)
patch_all()  # Path AWS libs with xray

# Reporting level, error - reject the whole file. warning - emit warning info but process the file (add columns, move to curated)
REPORT_ERROR = "error"
REPORT_WARNING = "warning"

# landing S3
## prefix for landing files
IN = "in"
## prefix for properly processed files
PROCESSED = "processed"
## prefix for invalid files
ERROR = "error"


# cureated S3
CURATED_BUCKET = "swmi-%s-sf-to-dw-curated"
## prefix for cureated
CURATED = "curated"

# Event bus config
EVT_BUS = os.getenv("EVENT_BUS", "default")
EVT_SOURCE = "au.com.code7swm.demo.ingest"
EVT_DETAIL_TYPE = "file-ingestion"

DEFAULT_REGION = "ap-southeast-2"

# Lineage columns to add to the curated files
FILENAME = "filename"
FUNCTION_NAME = "function name"
FUNCTION_VERSION = "function_version"
AWS_REQUEST_ID = "aws_request_id"
APPFLOW_ID = "appflow_id"
PROCESSED_TS = "processed_ts"

# DynamoDB config for schema
## table name
SCHEMA_TABLE = "swmi-%s-object-schema"
## range key source in the table. Only salesforce is used for this lambda.
SOURCE = "salesforce"


session = boto3.session.Session(region_name=os.getenv("AWS_REGION", DEFAULT_REGION))
s3 = session.resource("s3")
events = session.client("events")
lambda_client = session.client("lambda")
dynamodb = session.resource("dynamodb")


def get_params(lambda_client, context):
    """Get tag env from lambda. This must be dev/test/prod."""
    tags = lambda_client.list_tags(Resource=context.invoked_function_arn)
    env = tags.get("Tags", {}).get("env", "dev")
    version = tags.get("Tags", {}).get("version", "0.0.1")
    return env, version


def send_event(evt_details: dict):
    """Send an event to event bridge"""
    events.put_events(
        Entries=[
            {
                "EventBusName": EVT_BUS,
                "Detail": json.dumps(evt_details),
                "DetailType": EVT_DETAIL_TYPE,
                "Resources": [],
                "Source": EVT_SOURCE,
            }
        ]
    )


def partition() -> str:
    """returns a folder prefix partitioned by hour (UTC)"""
    return dt.utcnow().strftime("%Y/%m/%d/%H")


def get_object_details(source_bucket: str, source_key: str) -> (str, str):
    """Get object_name and filename from source key"""
    if not source_bucket:
        raise ValueError(f"This function only handles s3 bucket events")

    # Assume key:  in/<object_name>/<filename>
    key_parts = source_key.split("/")
    if len(key_parts) == 4 and (key_parts[0] == IN):
        object_name = re.sub("^[^-]+-+", "", key_parts[1])
        appflow_id = key_parts[2]
        filename = key_parts[3]
    else:
        raise ValueError(
            f"This function only handles objects in {IN}/<object>/<appflowid>/<filename>"
        )

    if object_name == "" or filename == "":
        raise ValueError(
            f"This function only handles objects in {IN}/<object>/<appflowid>/<filename>"
        )

    return (object_name, appflow_id, filename)


def process_parquet(
    appflow_id,
    source_bucket,
    source_key,
    context,
    version,
    read_table,
):
    """Read source parquet file, compare schemas and add extra columns"""
    # got_df = pq.read_table(f"s3://{source_bucket}/{source_key}")
    got_df = read_table(f"s3://{source_bucket}/{source_key}")

    ts = dt.utcnow()
    res_df = (
        got_df.append_column(
            FILENAME,
            pa.array([f"s3://{source_bucket}/{source_key}"] * len(got_df), pa.string()),
        )
        .append_column(
            FUNCTION_NAME, pa.array([context.function_name] * len(got_df), pa.string())
        )
        .append_column(FUNCTION_VERSION, pa.array([version] * len(got_df), pa.string()))
        .append_column(
            AWS_REQUEST_ID,
            pa.array([context.aws_request_id] * len(got_df), pa.string()),
        )
        .append_column(APPFLOW_ID, pa.array([appflow_id] * len(got_df), pa.string()))
        .append_column(
            PROCESSED_TS, pa.array([ts] * len(got_df), pa.timestamp("ns", "UTC"))
        )
    )
    return res_df


def do_handler(event, context, write_table, read_table):
    env, version = get_params(lambda_client, context)
    logger.info(f"Ingest lambda received event in {env}: {event}")

    curated_bucket = CURATED_BUCKET % env
    source_bucket = event.get("detail", {}).get("bucket", {}).get("name")
    source_key = event.get("detail", {}).get("object", {}).get("key")

    if (
        (not source_bucket)
        or (not source_key)
        or (len(source_bucket) == 0)
        or (len(source_key) == 0)
    ):
        logger.info(f"No file is added.")
        return

    try:
        (object_name, appflow_id, filename) = get_object_details(
            source_bucket, source_key
        )
    except:
        logger.warning(f"wrong folder structure {source_bucket}/{source_key}")
        return

    dst_filename = filename if filename.endswith(".parquet") else f"{filename}.parquet"

    pt = partition()
    try:
        df = process_parquet(
            appflow_id,
            source_bucket,
            source_key,
            context,
            version,
            read_table,
        )
        write_table(
            df,
            f"s3://{curated_bucket}/{object_name}/{pt}/{CURATED}_{dst_filename}",
            flavor="spark",
        )

        s3.Object(
            source_bucket, f"{PROCESSED}/{object_name}/{pt}/{appflow_id}/{filename}"
        ).copy_from(CopySource=f"{source_bucket}/{source_key}")
        s3.Object(source_bucket, source_key).delete()

    except (TypeError, ValueError, IOError, pa.ArrowException, Exception) as e:
        send_event(
            {
                "state": "error",
                "object": f"{source_bucket}/{source_key}",
                "message": str(e),
            }
        )
        s3.Object(
            source_bucket, f"{ERROR}/{object_name}/{pt}/{appflow_id}/{filename}"
        ).copy_from(CopySource=f"{source_bucket}/{source_key}")
        s3.Object(source_bucket, source_key).delete()
        raise e

    logger.info(f"successfully processed s3://{source_bucket}/{source_key}")


def handler(event, context):
    return do_handler(event, context, pq.write_table, pq.read_table)
