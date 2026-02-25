import json
from typing import List

from salesforce_bulk import SalesforceBulk, CsvDictsAdapter, UploadResult
from salesforce_bulk.util import IteratorBytesIO


def salesforce_run_query(sf_bulk: SalesforceBulk, job, batch) -> List:
    sf_bulk.wait_for_batch(job, batch)
    sf_bulk.close_job(job)
    items = []
    for result in sf_bulk.get_all_results_for_query_batch(batch):
        result = json.load(IteratorBytesIO(result))
        for row in result:
            items.append(row)
    return items


def salesforce_update(
    sf_bulk: SalesforceBulk, object_name: str, items
) -> List[UploadResult]:
    job = sf_bulk.create_update_job(object_name, contentType="CSV")
    csv_iter = CsvDictsAdapter(iter(items))
    batch = sf_bulk.post_batch(job, csv_iter)
    sf_bulk.wait_for_batch(job, batch)
    sf_bulk.close_job(job)
    return sf_bulk.get_batch_results(job_id=job, batch_id=batch)
