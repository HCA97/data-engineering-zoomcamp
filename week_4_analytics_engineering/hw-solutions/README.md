# HW 2 Solutions


## Q1

1. Use last weeks `etl_web_to_gcs_csv.py` to upload yellow and green trip data.
2. Create the bq tables.
    ```sql
    -- GREEN
    LOAD DATA OVERWRITE trips_data_all.green_tripdata
    FROM FILES (
    format = 'CSV',
    uris = [
        'gs://prefect-de-zoomcamp-1234/green_tripdata_2019-*.csv.gz',
        'gs://prefect-de-zoomcamp-1234/green_tripdata_2020-*.csv.gz'
    ]);

    -- YELLOW
    LOAD DATA OVERWRITE trips_data_all.yellow_tripdata
    FROM FILES (
    format = 'CSV',
    uris = [
        'gs://prefect-de-zoomcamp-1234/yellow_tripdata_2019-*.csv.gz',
        'gs://prefect-de-zoomcamp-1234/yellow_tripdata_2020-*.csv.gz'
    ]);
    ```
3. Copy use dbt to create fact tables [dbt-code](https://github.com/HCA97/de-zoomcamp-dbt)
    ```sql
    SELECT COUNT(*) AS number_fact_trips
    FROM `quiet-dimension-374920.dbt_test.fact_trips`
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2020, 2019)
    ```

Result
```
number_fact_trips 
61575997
```

## Q2

## Q3
```sql
SELECT COUNT(*) AS number_stg_fhv_trips
FROM `quiet-dimension-374920.dbt_test.stg_fhv_tripdata`
WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019)
```
Result
```
number_stg_fhv_trips 
43244696
```

## Q4

```sql
SELECT COUNT(*) AS number_fact_fhv_trips
FROM `quiet-dimension-374920.dbt_test.fact_fhv_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019)
```
Result
```
number_fact_fhv_trips 
22998722
```