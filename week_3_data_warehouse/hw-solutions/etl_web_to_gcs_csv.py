import tempfile as tmp
from datetime import timedelta
import os

from prefect_gcp.cloud_storage import GcsBucket
from prefect import task, flow
from prefect.tasks import task_input_hash

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def web_to_gcs(month: int, year: int, color: str) -> str:
    try:
        dataset_file = f"{color}_tripdata_{year}-{month:02}.csv.gz"
        dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}'
        os.system(f"wget {dataset_url} -O {dataset_file}")
        gcp_cloud_storage_bucket_block = GcsBucket.load("de-zoomcamp-bucket")
        gcp_cloud_storage_bucket_block.upload_from_path(from_path=dataset_file, to_path=dataset_file)
    finally:
        os.remove(dataset_file)

    return dataset_file

@flow(name='Prefect-ETL', log_prints=True)
def main_flow(months: list, year: int, color: str) -> None:
    dataset_names = [
         web_to_gcs.submit(month, year, color) for month in months
    ]
    print(dataset_names)

if __name__ == "__main__":
    months = list(range(1, 13))
    color = 'fhv'
    year = 2019
    main_flow(months, year, color)
