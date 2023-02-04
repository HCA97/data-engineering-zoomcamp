from datetime import timedelta
import os

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_gcs(color: str, year: int, month: int) -> pd.DataFrame:
    """Download trip data from GCS"""
    gcs_path = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcp_cloud_storage_bucket_block = GcsBucket.load("de-zoomcamp-bucket")
    gcp_cloud_storage_bucket_block.get_directory(from_path=gcs_path, local_path='../data')
    df = pd.read_parquet(f'../data/{gcs_path}')
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-zoomcamp-sa")

    df.to_gbq(
        destination_table="ny_taxi.yellow_ride",
        project_id=os.getenv('PROJECT_ID'),
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2019
    months = [2, 3]

    for month in months:
        df = extract_from_gcs(color, year, month)
        write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()