import json
import logging
import os
from aws_xray_sdk.core import xray_recorder
from dependency_injector.wiring import Provide, inject
from io import TextIOWrapper
from requests import RequestException
from simple_salesforce import Salesforce, SalesforceError

from app.csv_parser import parse, batch
from app.salesforce import (
    JOB_SUCCESS,
    JOB_FAIL,
    JobStatus,
    update_job_status,
    retrieve_file,
)
from salesforce_core.container import SfContainer, bootstrap as sf_bootstrap
from tech1_ci.container import Container as T1Container, bootstrap as t1_bootstrap
from tech1_ci.rpc import DoImportRequest, Tech1Exception

logger = logging.getLogger("lambda_handler")
logger.setLevel(logging.INFO)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))


@xray_recorder.capture("lambda_handler.handler")
@inject
def handler(
    event: dict,
    context: dict,
    sf: Salesforce = Provide[SfContainer.sf],
    do_import: DoImportRequest = Provide[T1Container.do_import],
):
    # Parse event for get job_id and file ids
    job_id = event["detail"]["IntegrationJobId__c"]
    file_ids = event["detail"]["FileIds__c"]
    logging.info(f"Received event for job {job_id} with file IDs {file_ids}")
    job_status = JobStatus(
        Status__c=JOB_FAIL,
    )
    # Get job obj id
    try:
        for file_id in json.loads(file_ids):
            # Get file from Salesforce
            raw = retrieve_file(sf, file_id)
            logger.info(
                f"Received {raw.getbuffer().nbytes} byes for file ID {file_id} from Salesforce"
            )
            # Parse CSV
            import_data = parse(TextIOWrapper(raw))
            logger.info(f"Parsed {len(import_data.rows)} rows for file ID {file_id}")
            for start, end in batch(len(import_data.rows), BATCH_SIZE):
                logger.info(f"Importing rows {start + 1}-{end} for {file_id}")
                # Import batch to Technology One
                do_import(import_data.columns, import_data.rows[start:end])
    except (SalesforceError, ValueError, RequestException, Tech1Exception) as e:
        job_status.Job_Message__c += str(e)
        update_job_status(sf, job_id, job_status)
        raise e

    job_status.Status__c = JOB_SUCCESS
    update_job_status(sf, job_id, job_status)


# To run locally ...
if __name__ == "__main__":
    xray_recorder.begin_segment(__name__)
    t1_bootstrap(modules=[handler.__module__])
    sf_bootstrap(modules=[handler.__module__])
    evt = {
        "version": "0",
        "id": "1b0a2510-a9f7-37f2-3443-af51e73b74b8",
        "detail-type": "BillingExtractReady__e",
        "source": "aws.partner/appflow/salesforce.com/[REDACTED_AWS_ACCOUNT_ID]/apidev-salesforce-events-BillingExtractReady__e",
        "account": "[REDACTED_AWS_ACCOUNT_ID]",
        "time": "2022-07-29T05:20:39Z",
        "region": "ap-southeast-2",
        "resources": [],
        "detail": {
            "CreatedDate": "2022-07-29T05:20:22.931Z",
            "CreatedById": "005Bm000000W6BxIAK",
            "FileIds__c": '["069Bm0000004VtGIAU"]',
            "IntegrationJobId__c": "a6iBm000000FZLdIAO",
        },
    }
    handler(evt, {})
