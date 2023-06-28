# Chambua Inc

This is an analytics engineering project that was initiated by a business stakeholder. The stakeholder is interested in getting some insights on the business. As the analytics engineer I have come up with an ETL data pipeline by

- [Extaracting data from aws S3 bucket with python to a postgres staging schema.](https://github.com/mukaruernest/data2bots/blob/main/README.md#extaract-data-from-aws-s3-bucket-with-python-to-a-postgres-staging-schema)
- [Using dbt(data build tool) transform and test the data.](https://github.com/mukaruernest/data2bots/blob/main/README.md#using-dbtdata-build-tool-transform-and-test-the-data)
- [Creating aggregate tables to generate insights.](https://github.com/mukaruernest/data2bots/blob/main/README.md#creating-aggregate-tables-and-generating-insights)
- [Export the transformed tables as csv files to aws data lake](https://github.com/mukaruernest/data2bots/blob/main/README.md#export-the-transformed-tables-as-csv-files-to-aws-data-lake)
- [Creating a visualization to show the insights needed by the business stakeholder.](https://github.com/mukaruernest/data2bots/blob/main/README.md#creating-a-visualization-to-show-the-insights-needed-by-the-business-stakeholder)

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

`stg_orders` table code
  
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

`stg_reviews` table code
  
```SQL
with reviews as(
    select 
        cast(review as integer),
        cast(product_id as varchar)
    from {{source ('chambua_inc', 'reviews')}}
)
select * from reviews
```

`stg_shipment_deliveries` table code
  
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

`dim_dates` table code  

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

## Creating Aggregate tables and generating insights.

One of the insights that the business stakeholder want to see are the total number of orders placed on a public holiday every month for the past year. To do that I implemented the `agg_public_holiday` table.

<details>
  <summary>click to view agg_public_holiday table code</summary>
	
```sql
with public_holidays as (
	select
		month_of_the_year_num
	from {{ref ('dim_dates')}}
	where (day_of_the_week_num between 1 and 5) and (work_day = False)
),
agg_orders as (
    select 
		extract(month from order_date) as month_of_the_year_num,
		count(order_id) as total_orders  
	from {{ref ('stg_orders')}}
	group by 1
)
select  
	cast(now() as date) as ingestion_date,
    count(case when a.month_of_the_year_num = 1 then true end) as tt_order_hol_jan,
    count(case when a.month_of_the_year_num = 2 then True end) as tt_order_hol_feb,
    count(case when a.month_of_the_year_num = 3 then True end) as tt_order_hol_mar,
    count(case when a.month_of_the_year_num = 4 then True end) as tt_order_hol_apr,
    count(case when a.month_of_the_year_num = 5 then True end) as tt_order_hol_may,
    count(case when a.month_of_the_year_num = 6 then True end) as tt_order_hol_jun,
    count(case when a.month_of_the_year_num = 7 then True end) as tt_order_hol_jul,
    count(case when a.month_of_the_year_num = 8 then True end) as tt_order_hol_aug,
    count(case when a.month_of_the_year_num = 9 then True end) as tt_order_hol_sep,
    count(case when a.month_of_the_year_num = 10 then True end) as tt_order_hol_oct,
    count(case when a.month_of_the_year_num = 11 then True end) as tt_order_hol_nov,
    count(case when a.month_of_the_year_num = 12 then True end) as tt_order_hol_dec
from agg_orders a 
inner join public_holidays d on d.month_of_the_year_num = a.month_of_the_year_num
```
</details>

Another insight that is import to the business stakeholder is the number of late shipments and the number undelivered shipments, for this I implemented the `agg_shipments` table.

<details>
  <summary>click to view agg_shipments table code</summary>	
	
```sql
with orders as (
    select * from {{ref ('stg_orders')}}
), shipments as (
    select * from {{ref ('stg_shipments_deliveries')}}
),
date_difference as (	
	select 
		sd.*,
		o.order_date,
		(sd.shipment_date - o.order_date) as late_delivery_date_difference,
		cast('2022-09-06' as date) -  o.order_date as undelivered_date_difference
	from shipments sd
	left join orders as o on o.order_id = sd.order_id
)
select 
	cast(now() as date) as ingestion_date,
	count (case when (late_delivery_date_difference >= 6) and (delivery_date is null) then true end) as tt_late_shipments,
	count (case when (delivery_date is null and shipment_date is null) and (undelivered_date_difference > 15) then true end) as tt_undelivered_shipmnets
from date_difference
```
</details>

Finally, product with the highest reviews , the day it was ordered the most, either that day was a public holiday , total review points, percentagedistribution of the review points , and percentage distribution of early shipments to late shipments for that particular product. For this I implemented the `best_performing_product` table.

<details>
  <summary>click to view best_performing_product table code</summary>

```sql
with 
orders as (
	select * from {{ref ('stg_orders')}}
),
reviews as (
	select * from {{ref ('stg_reviews')}}
),
dim_dates as (
	select * from {{ref ('dim_dates')}} 
),
agg_shipments as (
	select * from {{ref ('agg_shipments')}}
)
,total_reviews as(
	select 
		product_id, 
		sum(review) as total_reviews, 
		rank() over(order by sum(review) desc ) as ranking
	from reviews
	group by 1	 
	
)
,get_most_ordered_date as (
	select
		tr.product_id,
		o.order_date,
		total_reviews as total_review_points,
		count(o.order_id) as number_of_orders,
		rank() over(order by count(o.order_id) desc) as order_ranking
	from total_reviews tr
	left join orders o on o.product_id = tr.product_id
	where tr.ranking = 1
	group by 1,2,3
	order by 5
)
-- select sum(review) from reviews
select
	cast(now() as date) as ingestion_date,
	gmo.product_id,
	gmo.order_date,
	case when (day_of_the_week_num between 1 and 5) and (work_day = False) then True else false end as is_public_holiday,
	gmo.total_review_points,
	(gmo.total_review_points/(select sum(review) from reviews)) * 100 as pct_dist_review_points,
	(ag.tt_late_shipments/ag.tt_undelivered_shipmnets) * 100 as pct_dist_early_to_late_shipments
from get_most_ordered_date as gmo
left join dim_dates as d on gmo.order_date = d.calender_dt
left join agg_shipments ag on ag.ingestion_date = cast(now() as date)
where gmo.order_ranking = 1
group by 1,2,3,4,5,7
```	

</details>

To maintain data quality for the aggregate tables, ingestion_date test is used to assert that ingestion_date column is equal to the current date. Below is the macros used to create the ingestion_date test that is impkemented on a yml file
```sql
{% test ingestion_date(model, column_name) %}
with validation as (
    select
        {{ column_name }} as ingestion_date
    from {{ model }}
),
validation_errors as (
    select
        ingestion_date
    from validation
    -- if this is true, then ingestion date is not correct
    where ingestion_date != cast(now() as date)
)
select *
from validation_errors
{% endtest %}
```

To load the aggregate tables to an analytics schema I ran `dbt build -target prod` prod is configured on `profiles.yml` to load tables on analytics schema.

## Export the transformed tables as csv files to aws data lake.

This is done using terraform and python. Using python enures that there is scalability, maintability and reliability.

<details>
  <summary>click to view python code used to export aggregate tables to the data lake</summary>
	
```python
from python_terraform import Terraform
# import provider configurations
from config import aws_region, host, port, username, password
# Initialize Terraform configuration
tf = Terraform(working_dir='../chambua_inc')

# Define the table names
aggregates = ["agg_shipments", "agg_public_holiday", "agg_performing_product"]
version = ">= 1.19.0"
source = "cyrilgdn/postgresql"

# Define the Terraform configuration dynamically
configuration = f'''
terraform {{
  required_providers {{
    postgresql = {{
      version = "{version}"
      source  = "{source}"
    }}
  }}
}}
provider "aws" {{
  region = "{aws_region}"
}}

provider "postgresql" {{
  host     = "{host}"
  port     = {port}
  username = "{username}"
  password = "{password}"
}}

resource "aws_s3_bucket" "export_bucket" {{
  bucket = "your-export-bucket-name"
  acl    = "public-read"  # Adjust the ACL as per your requirements
}}

'''

# Generate the "data postgresql_table" blocks dynamically
for aggregate in aggregates:
    configuration += f'''
data "postgresql_table" "{aggregate}" {{
  schema = ""ernemuka4263_staging""
  name   = "{aggregate}"
}}
'''

# Generate the "resource aws_s3_bucket_object" blocks dynamically
for aggregate in aggregates:
    configuration += f'''
resource "aws_s3_bucket_object" "{aggregate}_export" {{
  bucket = aws_s3_bucket.export_bucket.id
  key    = "analytics_export/ernemuka4263/{aggregate}_export.csv"
  source = data.postgresql_table.{aggregate}.query_export_csv
}}
'''
# Load and apply the Terraform configuration
# tf.load_config(configuration)
tf.init()
tf.init()
tf.apply(skip_plan=True)
```
</details>

## Creating a visualization to show the insights needed by the business stakeholder.

A simple visualization displaying some of the insights.

![image](https://github.com/mukaruernest/data2bots/assets/10958742/6bd6ae5e-f9d0-4f50-bd9d-8d9a006bae17)



