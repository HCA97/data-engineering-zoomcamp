# HW 2 Solutions

## Q1

```bash
python etl_web_to_gcs.py q1
```

**Returns** `Total Number of Rows: 447770`

## Q2

M | H | D(moth) | M | D(week) 
0 | 5 |    1    | * | *

## Q3

1. Upload the data to BQ
```bash
export PROJECT_ID=$PROJECT_ID
python etl_web_to_gcs.py q3
python etl_gcs_to_bq.py
```

2. Count Rows
```sql
SELECT COUNT(*) 
FROM `quiet-dimension-374920.ny_taxi.yellow_ride`
```

**Returns:** 14851920

## Q4

1. Run the deploymehnt in the Prefect Cloud UI.

2. Run the flow in locally
```bash
prefect cloud login -k $API_KEY
prefect agent start  --work-queue "default"
```

**Retuns:** `22:32:14.906 | INFO    | Task run 'clean_data-decb1e9c-0' - Total Number of Rows: 88605`

## Q5

`Total Number of Rows: 514392`

## Q6

******** -> 8 digit