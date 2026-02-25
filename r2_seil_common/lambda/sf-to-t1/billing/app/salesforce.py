from io import BytesIO

import attr
import requests
from simple_salesforce import Salesforce

JOB_SUCCESS = "Received by TechOne"
JOB_FAIL = "Failed"
valid_statuses = (JOB_SUCCESS, JOB_FAIL)


@attr.define
class JobStatus:
    Status__c: str = attr.field()
    Job_Message__c: str = ""

    @Status__c.validator
    def check(self, attribute, value):
        if value not in valid_statuses:
            raise ValueError(f"Status__c not in {valid_statuses}")


# No longer required as the event seems to have changed to provide the ID instead of the name but leaving in case this
# is reverted in future
def find_job_id(sf: Salesforce, name: str) -> str:
    """Finds the object id of a job by name"""
    response = sf.query_all(
        f"SELECT Id FROM Integration_Job__c WHERE Name = '{name}'"  # nosec (this is not api exposed so low risk)
    )
    if len(response["records"]) != 1:
        raise ValueError(
            f"Expected one object ID for job {name}, got {len(response['records'])}"
        )
    return response["records"][0]["Id"]


def update_job_status(sf: Salesforce, job_id: str, status: JobStatus):
    """Updates the Integration_Job__c object with the status of a job"""
    sf.Integration_Job__c.update(job_id, attr.asdict(status))


def retrieve_file(sf: Salesforce, file_id: str) -> BytesIO:
    """Retrieves a file via Salesforce Connect API"""
    session = requests.Session()
    auth_id = "Bearer " + sf.session_id
    req_headers = {"Authorization": auth_id}
    resp = session.get(
        f"https://{sf.sf_instance}/services/data/v53.0/connect/files/{file_id}/content",
        headers=req_headers,
    )
    resp.raise_for_status()
    return BytesIO(resp.content)
