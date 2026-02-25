from typing import Any, List, Tuple
from xml.sax.saxutils import escape

import attr
from suds.client import Client


class Tech1Exception(Exception):
    pass


@attr.define
class Auth:
    """
    Authentication element for SOAP request

    _FunctionName should be set by method calling it as it varies between operations
    """

    _UserId: str
    _Password: str
    _Config: str

    def as_func(self, fn: str):
        return attr.asdict(self) | {"_FunctionName": fn}


@attr.define
class DoImportRequest:
    """
    Provides a callable DoImportRequest that takes a list of columns and a list of csv "encoded" rows
    """

    wsdl: str
    auth: Auth
    warehouse_name: str
    warehouse_table_name: str

    def __call__(self, columns: List[str], rows: List[str]):
        client, req = self._build_request(columns, rows)
        resp = client.service.Warehouse_DoImport(req)
        if resp.Errors._IsError:
            raise Tech1Exception(str(resp.Errors.Items))

    def _build_request(self, columns: List[str], rows: List[str]) -> Tuple[Client, Any]:
        client = Client(self.wsdl)
        safe_rows = [escape(r) for r in rows]
        req = client.factory.create("Warehouse_DoImport_Request")
        req.Auth = self.auth.as_func("$E1.BI.WHT.DOIMP.WS")
        req._WarehouseName = self.warehouse_name
        req._WarehouseTableName = self.warehouse_table_name
        req._ImportMode = "InsertMode"
        req.Columns = client.factory.create("Warehouse_DoImport_Request.Columns")
        req.Columns.ColumnInfo = [{"_Name": c} for c in columns]
        req.Rows = client.factory.create("ArrayOfString")
        req.Rows.Row = safe_rows
        return client, req
