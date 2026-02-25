from app.clients.report_client import ReportClient

REPORT_SAMPLE_RESPONSE = {
    "results": [{"id": "5062505", "name": "order-name-1", "status": "PAUSED"}]
}


class ReportClientMock(ReportClient):
    def __init__(self, config):
        super().__init__(config, ad_manager_client=self)

    def GetDataDownloader(self, version):
        return self

    def WaitForReport(self, request):
        return []

    def DownloadReportToFile(
        self, report_job_id, export_format, report_file, use_gzip_compression=False
    ):
        return []
