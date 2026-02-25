import json
from typing import List
import pandas as pd
import pytest
from botocore.stub import Stubber
from io import IOBase, BytesIO
from freezegun import freeze_time
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime as dt
import moto
from app.ingest import (
    partition,
    get_object_details,
    FILENAME,
    FUNCTION_NAME,
    FUNCTION_VERSION,
    AWS_REQUEST_ID,
    APPFLOW_ID,
    PROCESSED_TS,
    process_parquet,
)


@pytest.mark.parametrize(
    "t,expected",
    [
        ("1970/01/01 12:00:00am", "1970/01/01/00"),
        ("2022/12/31 23:59:59", "2022/12/31/23"),
    ],
)
def test_partition(t, expected):
    with freeze_time(t):
        assert partition() == expected


def init_schema_dynamodb(schema, dynamodb):
    table_name = "swmi-dev-object-schema"

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "object", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "source", "KeyType": "RANGE"},  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": "object", "AttributeType": "S"},
            {"AttributeName": "source", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )

    with table.batch_writer() as batch:
        batch.put_item(Item=schema)


@pytest.mark.parametrize(
    "source_bucket,source_key,expected",
    [
        (
            "mybucket1",
            "in/dev-Salesforce-obj1/uuid1/d_1.parquet",
            ("Salesforce-obj1", "uuid1", "d_1.parquet"),
        ),
        pytest.param("mybucket2", "in/d2.parquet", (), marks=pytest.mark.xfail),
        pytest.param("mybucket2", "da/object2/d2.parquet", (), marks=pytest.mark.xfail),
    ],
)
def test_get_object_details(source_bucket, source_key, expected):
    got = get_object_details(source_bucket, source_key)
    assert expected == got


def init_proces_parquet(s3, data, fields, cxt, ts):
    """Create a bucket with parquet file, create ddb table with schema"""
    rowcnt = len(data)
    colcnt = len(data[0])
    table_data = [pa.array([data[i][j] for i in range(rowcnt)]) for j in range(colcnt)]
    schema = pa.schema([pa.field(c, t, n) for (c, t, n) in fields])
    table = pa.Table.from_arrays(table_data, schema=schema)

    # create bucket swmi-dev-sf-to-dw-landing
    source_bucket = "swmi-dev-sf-to-dw-landing"
    s3.create_bucket(
        Bucket=source_bucket,
    )

    write_table = mock_s3_write_table(s3)

    # create parquet file
    source_key = "in/mytable/mydata.paquet"
    write_table(table, f"s3://{source_bucket}/{source_key}")

    appflow_id = "dummy_appflow_id"

    context = type(
        "",
        (object,),
        {"function_name": cxt[0], "function_version": cxt[1], "aws_request_id": cxt[2]},
    )()
    version = context.function_version
    expected = (
        table.append_column(
            FILENAME,
            pa.array([f"s3://{source_bucket}/{source_key}"] * len(table), pa.string()),
        )
        .append_column(
            FUNCTION_NAME, pa.array([context.function_name] * len(table), pa.string())
        )
        .append_column(FUNCTION_VERSION, pa.array([version] * len(table), pa.string()))
        .append_column(
            AWS_REQUEST_ID, pa.array([context.aws_request_id] * len(table), pa.string())
        )
        .append_column(APPFLOW_ID, pa.array([appflow_id] * len(table), pa.string()))
        .append_column(
            PROCESSED_TS, pa.array([ts] * len(table), pa.timestamp("ns", "UTC"))
        )
    )

    return (
        appflow_id,
        source_bucket,
        source_key,
        context,
        version,
        expected,
    )


@pytest.mark.parametrize(
    "data,fields,cxt",
    [
        (
            [(1, "2", 3.0), (4, "5", 6.3), (7, "8", 9.3)],
            [("c1", "int32", True), ("c2", "string", False), ("c3", "float64", False)],
            ["swmi-def-sf-to-dw-ingest", "1.0.1", "runid_20202020"],
        )
    ],
)
@freeze_time("2022/12/31 23:59:59")
def test_process_parquet(data, fields, cxt, s3, dynamodb):
    ts = dt.now()
    (
        appflow_id,
        source_bucket,
        source_key,
        context,
        version,
        expected,
    ) = init_proces_parquet(s3, data, fields, cxt, ts)
    read_table = mock_s3_read_table(s3)
    got = process_parquet(
        appflow_id,
        source_bucket,
        source_key,
        context,
        version,
        read_table,
    )
    assert expected == got


def split_s3_path(s3path):
    pps = s3path.replace("s3://", "").split("/")
    bucket = pps.pop(0)
    key = "/".join(pps)
    return (bucket, key)


def mock_s3_write_table(s3):
    def do_mock_s3_write_table(table, s3path, flavor=None):
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf, flavor)
        (bucket, key) = split_s3_path(s3path)
        obj = s3.Object(bucket, key)
        obj.put(Body=buf.getvalue().to_pybytes())

    return do_mock_s3_write_table


def mock_s3_read_table(s3):
    def do_mock_s3_read_table(s3path):
        (bucket, key) = split_s3_path(s3path)
        obj2 = s3.Object(bucket, key)
        buf = obj2.get()["Body"].read()
        return pq.read_table(BytesIO(buf))

    return do_mock_s3_read_table
