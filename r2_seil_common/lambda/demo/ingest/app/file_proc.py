""" Demo event based lambda to convert CSV to Parquet """
import json
import logging
import os
from datetime import datetime
from io import IOBase, BytesIO
from time import time

import boto3
import pytz
from aws_xray_sdk.core import patch_all
from pyarrow import csv, parquet, ArrowException

logger = logging.getLogger()
logger.setLevel(logging.INFO)
patch_all()  # Path AWS libs with xray

IN = "in/"
PROCESSED = "processed/"
OUT = "out/"
ERROR = "error/"
EVT_BUS = os.getenv("EVENT_BUS", "default")
EVT_SOURCE = "au.com.code7swm.demo.ingest"
EVT_DETAIL_TYPE = "csv-ingestion"

session = boto3.session.Session()
s3 = session.client("s3")
events = session.client("events", region_name=os.getenv("AWS_REGION", "ap-southeast-2"))


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


def read_file(bucket: str, key: str) -> (IOBase, str):
    """read an object from s3 and return the bytes along with the object key without the prefix"""
    if not key.startswith(IN):
        raise ValueError(f"This function only handles objects in {IN}")
    return s3.get_object(Bucket=bucket, Key=key)["Body"], key[len(IN) :]


def csv_to_parquet(source: IOBase) -> IOBase:
    """converts byte stream containing a CSV to parquet"""
    output = BytesIO()
    parquet.write_table(csv.read_csv(source), output)
    return output


def partition() -> str:
    """returns a folder prefix partitioned by hour"""
    dt = datetime.fromtimestamp(time(), tz=pytz.UTC)
    return f"{dt.year}/{dt.month:02d}/{dt.day:02d}/{dt.hour:02d}/"


def handler(event, context):
    bucket = event["detail"]["bucket"]["name"]
    key = event["detail"]["object"]["key"]
    logger.info(f"Attempting to download {bucket}/{key}")
    obj_reader, obj_name = read_file(bucket, key)
    data = obj_reader.read()
    pt = partition()
    try:
        pq = csv_to_parquet(BytesIO(data))
        dst_obj = f"{OUT}{pt}{obj_name}.parquet"
        logger.info(f"Writing to {dst_obj}")
        s3.put_object(Bucket=bucket, Key=dst_obj, Body=pq.read())
        s3.put_object(Bucket=bucket, Key=f"{PROCESSED}{pt}{obj_name}", Body=data)
        s3.delete_object(Bucket=bucket, Key=key)
        send_event({"state": "success", "object": f"{bucket}/{key}"})
    except (TypeError, ValueError, IOError, ArrowException) as e:
        s3.put_object(Bucket=bucket, Key=f"{ERROR}{pt}{obj_name}", Body=data)
        s3.delete_object(Bucket=bucket, Key=key)
        send_event({"state": "error", "object": f"{bucket}/{key}", "message": str(e)})
        raise e
