# Chambua Inc

This is an analytics engineering project that was initiated by a business stakeholder. The stakeholder is interested in getting some insights on the business. As the analytics engineer I have come up with an ETL data pipeline by

- Extaracting data from aws S3 bucket with python to a postgres staging schema.
- Using dbt(data build tool) transform and test the data.
- Loading the transformed tables to an analytics schema that can be accessed by stakeholders.
- Export the transformed tables as csv files to aws data lake
- Creating a visualization to show the insights needed by the business stakeholder.

## Extaract data from aws S3 bucket with python to a postgres staging schema.

Below is python the code I used to load the raw data into the staging schema in postgres. The code ensures there is scalability, maintability and reliability
<details>
  <summary>Click here to view python code on drop down</summary>
  
```python
# Code to extract data from s3 bucket to postgres. 
import boto3
import psycopg2
import pandas as pd
import os

from botocore import UNSIGNED
from botocore.client import Config
from config import host, port, username, password
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
bucket_name = "d2b-internal-assessment-bucket"
response = s3.list_objects(Bucket=bucket_name, Prefix="orders_data")

file_names = ['orders', 'reviews','shipment_deliveries']
prefix="orders_data"
chambua = {}
for file_name in file_names:
  s3.download_file(bucket_name, f"{prefix}/{file_name}.csv", f"{file_name}.csv")
  chambua[file_name] = pd.read_csv(f"{file_name}.csv")

for table in file_names:
  dataframe = chambua[table]
  table_name = table
  column_names = dataframe.columns
  replacements = {
  "object": "VARCHAR",
  "int64": "INTEGER",
  "float64": "NUMERIC",
  "bool": "BOOLEAN",
  "datetime64[ns]": "TIMESTAMP",
  "datetime64[ns, UTC]": "TIMESTAMP WITH TIME ZONE",
  "timedelta64[ns]": "INTERVAL",
  "category": "VARCHAR",
  "UInt8": "SMALLINT",
  "UInt16": "SMALLINT",
  "UInt32": "INTEGER",
  "UInt64": "BIGINT",
  "Int8": "SMALLINT",
  "Int16": "SMALLINT",
  "Int32": "INTEGER",
  "Int64": "BIGINT",
  "float16": "NUMERIC",
  "float32": "NUMERIC",
  "float64": "NUMERIC",
  "bool_": "BOOLEAN",
  "datetime64": "TIMESTAMP",
  "timedelta64": "INTERVAL"
  }

  col_str = ", ".join(["{} {}".format(n, d) for (n, d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements))])

  # Connect to the PostgreSQL database
  connection = psycopg2.connect(
  host= host,
  port=	port,
  user=username,
  password=password,
  database="d2b_accessment",
  )
  cursor = connection.cursor()
  schema_name="ernemuka4263_staging"
  # drop table with the same name
  cursor.execute(f"drop table if exists {schema_name}.{table_name}")
  #create table
  cursor.execute(f"create table {schema_name}.{table_name} ({col_str})")
  #open file in memory
  dataframe.to_csv(table, header=column_names, index=False, encoding='utf-8')
  #opn csv and save it as an object
  chambua_data = open(table)

  SQL_QUERY = """
  COPY {0} FROM STDIN WITH
  CSV
  HEADER
  DELIMITER ','
  """

  cursor.copy_expert(SQL_QUERY.format(f"{schema_name}.{table_name}"), chambua_data)

  connection.commit()
  connection.close()
```

</details>

## Using dbt(data build tool) transform and test the data.

I begin the data transformation process by initiating a dbt project. I create a staging area with `stg_orders`, `stg_reviews`, and `stg_shipment_deliveries` tables by selecting from the source file as shown below. This ensures that the data coming in is clean.

The source file contains `unique` and `not_null` tests for primary keys to avoid inacurrate data. 
  
```yml
version: 2

sources: 
  - name: chambua_inc
    description: raw data from the staging schema
    database: d2b_accessment  
    schema: ernemuka4263_staging  
    tables:
      - name: orders
        description: this is the orders table showing each order that was made.
        columns:
          - name: order_id
            description: the primary key for the orders table
            tests:
              - unique
              - not_null
      - name: reviews
      - name: shipment_deliveries
        columns:
          - name: shipment_id 
            tests:
              - unique
              - not_null
```

stg_orders table code
  
```SQL
  with orders as (
    select 
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        cast(product_id as varchar) as product_id,
        unit_price,
        quantity,
        total_price as amount
    from {{source ('chambua_inc', 'orders')}}
)
select * from orders
```

stg_reviews table code
  
```SQL
with reviews as(
    select 
        cast(review as integer),
        cast(product_id as varchar)
    from {{source ('chambua_inc', 'reviews')}}
)
select * from reviews
```

stg_shipment_deliveries table code
  
```SQL
with shipment_deliveries as (
    select 
        shipment_id,
        order_id,
        cast(shipment_date as date) as shipment_date,
        cast(delivery_date as date) as delivery_date
    from {{source ('chambua_inc', 'shipment_deliveries')}}
)
select * from shipment_deliveries
```

From the staging tables dim_dates table to extract day, month, and year numbers from order date and also to come up with a formular to check if the day is a work_day.

stg_shipment_deliveries table code  

```SQL
with order_date as (
    select
        order_date
    from {{ref ('stg_orders')}}
), date_numbers as(
    select
        distinct order_date as calender_dt,
        extract(year from order_date) as year_num,
        extract(month from order_date) as month_of_the_year_num,
        extract(day from order_date) as day_of_the_month_num,
        extract(isodow from order_date) as day_of_the_week_num
    from order_date
), working_day_bool_logic as (
    select
        *,
        case when (day_of_the_week_num between 1 and 5) then True else False end as work_day
    from date_numbers
)
select * from working_day_bool_logic
```


