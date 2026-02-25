import json
from io import BytesIO
from typing import List

import pandas as pd
import pytest
from aws_xray_sdk.core import xray_recorder
from botocore.stub import Stubber
from freezegun import freeze_time
from pyarrow import csv, parquet, ArrowException

from app.file_proc import (
    csv_to_parquet,
    partition,
    read_file,
    s3,
    handler,
    events,
    EVT_BUS,
    EVT_DETAIL_TYPE,
    EVT_SOURCE,
)

mock_csv = {
    "test1": "id,first_name,last_name,email,gender,ip_address\n"
    "1,Scott,Gwilliams,sgwilliams0@cam.ac.uk,Female,114.191.215.198\n"
    "2,Cosette,Blow,cblow1@xing.com,Male,217.175.111.84\n"
    "3,Kinsley,Veld,kveld2@sbwire.com,Agender,115.214.159.119\n"
    "4,Boot,Eastmond,beastmond3@cmu.edu,Male,244.122.23.103\n"
    "5,Winslow,Cashman,wcashman4@w3.org,Female,138.6.12.16\n"
    "6,Ban,Izaks,bizaks5@amazon.com,Female,112.4.148.32\n"
    "7,Sela,Gianettini,sgianettini6@people.com.cn,Female,242.156.23.70\n"
    "8,Lorene,Eyton,leyton7@nps.gov,Female,254.149.0.28\n"
    "9,Lisbeth,Mustin,lmustin8@cdc.gov,Male,251.182.123.195\n".encode("utf-8")
}


def to_pq(b: bytes) -> bytes:
    pq = BytesIO()
    parquet.write_table(csv.read_csv(BytesIO(b)), pq)
    return pq.read()


mock_pq = {k: to_pq(mock_csv[k]) for k in mock_csv.keys()}


@pytest.fixture(scope="function", autouse=True)
def segment():
    xray_recorder.begin_segment("test")


@pytest.fixture(scope="function")
def s3_stub():
    with Stubber(s3) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture(scope="function")
def evt_stub():
    with Stubber(events) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize(
    "in_file,rows,columns",
    [("test1", 9, ("id,first_name,last_name,email,gender,ip_address".split(",")))],
)
def test_csv_to_parquet(in_file: str, rows: int, columns: List[str]):
    in_csv = BytesIO(mock_csv[in_file])
    out_parquet = csv_to_parquet(in_csv)
    df = pd.read_parquet(out_parquet)
    assert len(df) == rows
    assert list(df.columns) == list(columns)


@pytest.mark.parametrize(
    "t,expected",
    [
        ("1970/01/01 12:00:00am", "1970/01/01/00/"),
        ("2022/12/31 23:59:59", "2022/12/31/23/"),
    ],
)
def test_partition(t, expected):
    with freeze_time(t):
        assert partition() == expected


def test_read_file(s3_stub):
    s3_stub.add_response(
        "get_object",
        expected_params={"Bucket": "foo", "Key": "in/bar"},
        service_response={"Body": BytesIO()},
    )
    result = read_file("foo", "in/bar")
    assert result[1] == "bar"
    with pytest.raises(ValueError):
        read_file("foo", "bar")


def test_handler(s3_stub, evt_stub):
    with freeze_time("2022/01/01 00:00:00"):
        # Happy path
        s3_stub.add_response(
            "get_object",
            expected_params={"Bucket": "foo", "Key": "in/bar"},
            service_response={"Body": BytesIO(mock_csv["test1"])},
        )
        s3_stub.add_response(
            "put_object",
            expected_params={
                "Bucket": "foo",
                "Key": "out/2022/01/01/00/bar.parquet",
                "Body": mock_pq["test1"],
            },
            service_response={},
        )
        s3_stub.add_response(
            "put_object",
            expected_params={
                "Bucket": "foo",
                "Key": "processed/2022/01/01/00/bar",
                "Body": mock_csv["test1"],
            },
            service_response={},
        )
        s3_stub.add_response(
            "delete_object",
            expected_params={"Bucket": "foo", "Key": "in/bar"},
            service_response={},
        )
        evt_stub.add_response(
            "put_events",
            expected_params={
                "Entries": [
                    {
                        "EventBusName": EVT_BUS,
                        "DetailType": EVT_DETAIL_TYPE,
                        "Resources": [],
                        "Source": EVT_SOURCE,
                        "Detail": json.dumps(
                            {"state": "success", "object": "foo/in/bar"}
                        ),
                    }
                ]
            },
            service_response={},
        )
        handler(
            {"detail": {"bucket": {"name": "foo"}, "object": {"key": "in/bar"}}}, None
        )

        # Sad path
        with pytest.raises(ArrowException):
            s3_stub.add_response(
                "get_object",
                expected_params={"Bucket": "foo", "Key": "in/bar"},
                service_response={"Body": BytesIO(b"")},  # Empty CSV
            )
            s3_stub.add_response(
                "put_object",
                expected_params={
                    "Bucket": "foo",
                    "Key": "error/2022/01/01/00/bar",
                    "Body": b"",
                },
                service_response={},
            )
            s3_stub.add_response(
                "delete_object",
                expected_params={"Bucket": "foo", "Key": "in/bar"},
                service_response={},
            )
            evt_stub.add_response(
                "put_events",
                expected_params={
                    "Entries": [
                        {
                            "EventBusName": EVT_BUS,
                            "DetailType": EVT_DETAIL_TYPE,
                            "Resources": [],
                            "Source": EVT_SOURCE,
                            "Detail": json.dumps(
                                {
                                    "state": "error",
                                    "object": "foo/in/bar",
                                    "message": "Empty CSV file",
                                }
                            ),
                        }
                    ]
                },
                service_response={},
            )
            handler(
                {"detail": {"bucket": {"name": "foo"}, "object": {"key": "in/bar"}}},
                None,
            )
