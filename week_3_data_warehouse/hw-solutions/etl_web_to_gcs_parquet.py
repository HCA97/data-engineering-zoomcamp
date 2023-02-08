import tempfile as tmp
from datetime import timedelta
import os

import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
from prefect import task, flow
from prefect.tasks import task_input_hash


def download_data(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path)
    

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    return df

def upload_data_parquet(df: pd.DataFrame, dataset_name: str) -> str:
    gcp_cloud_storage_bucket_block = GcsBucket.load("de-zoomcamp-bucket")
    with tmp.NamedTemporaryFile(suffix='.parquet') as f:
        df.to_parquet(f.name, compression="gzip")
        print(f'Succesfully save the file to {f.name}')
        gcp_cloud_storage_bucket_block.upload_from_path(from_path=f.name, to_path=dataset_name)
    return dataset_name

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def pd_to_gcs(month: int, year: int, color: str) -> str:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
    
    df = download_data(dataset_url)
    df = clean_data(df)
    upload_data_parquet(df, f'{dataset_file}.parquet')
    return dataset_file

@flow(name='Prefect-ETL', log_prints=True)
def main_flow(months: list, year: int, color: str) -> None:
    dataset_names = [
         pd_to_gcs.submit(month, year, color) for month in months
    ]
    print(dataset_names)

if __name__ == "__main__":
    months = list(range(1, 13))
    color = 'fhv'
    year = 2019
    main_flow(months, year, color)

