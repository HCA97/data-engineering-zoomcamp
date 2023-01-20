# HW 1 Solutions

## Q1

```bash
docker build --help | grep 'Write the image ID to the file'
```

Results

```text
result =       --iidfile string          Write the image ID to the file
```

## Q2

Dockerfile

```Docker
FROM python:3.9

ENTRYPOINT [ "/bin/bash" ]
```

Build & Run

```bash
docker build . -t de-zoomcamp-hw1-python
docker run -it de-zoomcamp-hw1-python
```

Run  `pip list` in docker image.

Results

```text
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
```

## Q3

```bash
cd week_1_basics_n_setup/2_docker_sql
```

Start Postgres DB

```bash
docker-compose up
```

Setup

```bash
sudo apt-get install wget
pip install pandas sqlalchemy psycopg2
```

Ingest Green Trip Data

```bash
python ingest_data.py \
    --user root  \
    --password root \
    --host localhost \
    --port 5432 \
    --db ny_taxi \
    --table_name green_trip \
    --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
```

Ingest Zones Data

```bash
python ingest_data.py \
    --user root  \
    --password root \
    --host localhost \
    --port 5432 \
    --db ny_taxi \
    --table_name zones_lookup \
    --url https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

SQL Query to Retrieve Taxi Trips

```SQL
SELECT COUNT(*) AS taxi_trips_2019_01_15
FROM green_trip
WHERE lpep_pickup_datetime::date = '2019-01-15'::date
  AND lpep_dropoff_datetime::date = '2019-01-15'::date
```

Result

```text
taxi_trips_2019_01_15
20530
```

## Q4

Same setup same as [Q3](#q3)

SQL Query

```SQL
SELECT lpep_pickup_datetime::date as day_with_largest_trip
FROM green_trip
ORDER BY trip_distance DESC
LIMIT 1
```

Result

```text
day_with_largest_trip
2019-01-15
```

## Q5

Same setup same as [Q3](#q3)

SQL Query

```SQL
SELECT passenger_count, count(*) as number_of_trips
FROM green_trip
WHERE (lpep_pickup_datetime::date = '2019-01-01'::date
   OR lpep_dropoff_datetime::date = '2019-01-01'::date)
  AND passenger_count IN (2, 3)
GROUP BY passenger_count
```

Result

```text
passenger_count,number_of_trips
2,1282
3,254
```

## Q6

Same setup same as [Q3](#q3)

SQL Query

```SQL
SELECT zd."Zone"
FROM green_trip as gt
JOIN zones_lookup as zp
  ON gt."PULocationID" = zp."LocationID"
JOIN zones_lookup as zd
  ON gt."DOLocationID" = zd."LocationID"
WHERE zp."Zone" = 'Astoria'
ORDER BY gt.tip_amount DESC
LIMIT 1
```

Result

```text
Zone
Long Island City/Queens Plaza
```