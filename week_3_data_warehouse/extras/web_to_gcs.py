import io
import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dbt-ny-taxi-123")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def load_csv(i, year, service):
    # sets the month part of the file_name string
    month = '0'+str(i+1)
    month = month[-2:]

    # csv file_name 
    file_name = f"{service}_tripdata_{year}-{month:02}.csv.gz"

    # download it using requests via a pandas df
    request_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file_name}'
    r = requests.get(request_url)
    pd.DataFrame(io.StringIO(r.text)).to_csv(file_name)
    print(f"Local: {file_name}")

    # upload it to gcs 
    upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
    print(f"GCS: {service}/{file_name}")

def web_to_gcs(year, service):
    print(f'{year} - {service}')
    with ThreadPoolExecutor(max_workers=12) as executor:
        for i in range(12):
            executor.submit(load_csv, i, year, service)

web_to_gcs('2019', 'green')
web_to_gcs('2020', 'green')
web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')
