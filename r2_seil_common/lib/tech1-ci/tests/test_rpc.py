import pathlib

import os

from tech1_ci.rpc import Auth, DoImportRequest


def test_auth():
    auth = Auth(UserId="user", Password="password", Config="config")
    assert auth.as_func("func") == {
        "_UserId": "user",
        "_Password": "password",
        "_Config": "config",
        "_FunctionName": "func",
    }


def test_do_request():
    wsdl_path = pathlib.Path(os.path.join(os.path.dirname(__file__), "wsdl")).as_uri()
    req_callable = DoImportRequest(
        wsdl=wsdl_path,
        auth=Auth(UserId="user", Password="password", Config="config"),
        warehouse_name="wh_name",
        warehouse_table_name="wh_table",
    )
    client, req = req_callable._build_request(["a", "b", "c"], ["1,2,3", ">4,5&6,7"])
    # Function name should be set in Auth
    assert req.Auth["_FunctionName"] == "$E1.BI.WHT.DOIMP.WS"
    # Verify data lengths and XML escaping
    assert len(req.Columns.ColumnInfo) == 3
    assert len(req.Rows.Row) == 2
    assert req.Rows.Row[1] == "&gt;4,5&amp;6,7"
