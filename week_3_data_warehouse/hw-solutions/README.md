
# Q1 

1- Create table using BQ UI

2- Run SQL Query
```sql
SELECT COUNT(*) AS Num_Rows
FROM `ny_taxi.fhv_trip`
```
**Result:** 43244696

# Q2

1- Create External Table
```sql
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.fhv_trip_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-1234/fhv_tripdata_2019-*.csv.gz']
);
```
2- Run SQL Queries

```sql
-- 0MB because data is in GCS
SELECT COUNT(DISTINCT Affiliated_base_number) AS Num_Rows
FROM `ny_taxi.fhv_trip_external`
```

```sql
-- 317.94MB
SELECT COUNT(DISTINCT Affiliated_base_number) AS Num_Rows
FROM `ny_taxi.fhv_trip`
```

# Q3

```sql
SELECT COUNT(*) AS Num_Rows
FROM `ny_taxi.fhv_trip`
WHERE PUlocationID IS NULL
  AND DOlocationID IS NULL
```
**Result** 717748

# Q4

`Partition by pickup_datetime Cluster on affiliated_base_number` 

# Q5

1. Create Optimized Table
```sql
CREATE OR REPLACE TABLE `ny_taxi.fhv_trip_optimized`
PARTITION BY DATE(pickup_datetime	)
CLUSTER BY Affiliated_base_number AS (
  SELECT * FROM `ny_taxi.fhv_trip`
);
```

2- Run SQL Queries

```sql
-- 23.06 MB
SELECT COUNT(DISTINCT Affiliated_base_number) AS Num_Rows
FROM `ny_taxi.fhv_trip_optimized`
WHERE DATE(pickup_datetime) BETWEEN DATE('2019-03-01') AND DATE('2019-03-31')
```

```sql
-- 647.87 MB
SELECT COUNT(DISTINCT Affiliated_base_number) AS Num_Rows
FROM `ny_taxi.fhv_trip`
WHERE DATE(pickup_datetime) BETWEEN DATE('2019-03-01') AND DATE('2019-03-31')
```

# Q6

GCP Bucket

# Q7

False

# Q8

Column types are not matched.