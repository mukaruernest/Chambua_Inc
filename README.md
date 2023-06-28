# Chambua Inc

This is an analytics engineering project that was initiated by a business stakeholder. The stakeholder is interested in getting some insights on.

1. The total number of orders placed on a public holiday every month for the past year
2. Total number of late shipments
3. Total number of undelivered shipments
4. The product with the highest reviews , the day it was ordered the most, either that day was a public holiday , total review points, percentage
distribution of the review points , and percentage distribution of early shipments to late shipments for that particular product.

To get the insight I follwed the following steps

1. Extaract data from aws S3 bucket with python to a postgres staging schema.
2. Using dbt(data build tool) transform and test the data.
3. Load the transformed tables to an analytics schema.
4. Export the transformed tables as csv files to an aws.
5. Create a visualization to show the insights needed by the business stakeholder.

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

Once I have the data on the staging schema, I initiate a dbt project and create a staging area for all the three tables by selecting from the source file as shown below.

<details>
  <summary>stg_orders code</summary>
  
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

</details>

<details>
  <summary>stg_reviews code</summary>
  
```SQL
with reviews as(
    select 
        cast(review as integer),
        cast(product_id as varchar)
    from {{source ('chambua_inc', 'reviews')}}
)
select * from reviews
```

</details>

<details>
  <summary>stg_shipment_deliveries code</summary>
  
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

</details>


