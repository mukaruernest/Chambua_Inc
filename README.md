# Chambua Inc

This is an analytics engineering project that was initiated by a business stakeholder. The stakeholder is interested in getting some insights on the business. As the analytics engineer I have come up with an ETL data pipeline by;

- [Extaracting data from aws S3 bucket with python to a postgres staging schema.](https://github.com/mukaruernest/data2bots/blob/main/README.md#extaract-data-from-aws-s3-bucket-with-python-to-a-postgres-staging-schema)
- [Using dbt(data build tool) transform and test the data.](https://github.com/mukaruernest/data2bots/blob/main/README.md#using-dbtdata-build-tool-transform-and-test-the-data)
- [Creating aggregate tables to generate insights.](https://github.com/mukaruernest/data2bots/blob/main/README.md#creating-aggregate-tables-and-generating-insights)
- [Export the transformed tables as csv files to aws data lake](https://github.com/mukaruernest/data2bots/blob/main/README.md#export-the-transformed-tables-as-csv-files-to-aws-data-lake)
- [Creating a visualization to show the insights needed by the business stakeholder.](https://github.com/mukaruernest/data2bots/blob/main/README.md#creating-a-visualization-to-show-the-insights-needed-by-the-business-stakeholder)

![image](https://github.com/mukaruernest/data2bots/assets/10958742/44e3ba2a-1888-4a7f-81a9-5b296fc8a266)


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
with orders as (
	select
		extract(month from order_date) as month_of_the_year_num,
		extract(isodow from order_date) as day_of_the_week_num,
		count(order_id) as total_orders
	from {{ref ('stg_orders')}}
	group by 1,2
),dim_dates as(
	select * from {{ref ('dim_dates')}}
), total_orders as (
    select 
        cast(now() as date) as ingestion_date,
        o.month_of_the_year_num,
        o.day_of_the_week_num,
        count(total_orders) as total_order
    from orders o
    left join dim_dates as d on d.month_of_the_year_num = (o.month_of_the_year_num)
    where (d.work_day = False) and (o.day_of_the_week_num between 1 and 5)
    group by 1,2,3
)
select 
ingestion_date,
sum(case when month_of_the_year_num = 1 then total_order end ) as tt_order_hol_jan,
sum(case when month_of_the_year_num = 2 then total_order end ) as tt_order_hol_feb,
sum(case when month_of_the_year_num = 3 then total_order end ) as tt_order_hol_mar,
sum(case when month_of_the_year_num = 4 then total_order end ) as tt_order_hol_apr,
sum(case when month_of_the_year_num = 5 then total_order end ) as tt_order_hol_may,
sum(case when month_of_the_year_num = 6 then total_order end ) as tt_order_hol_jun,
sum(case when month_of_the_year_num = 7 then total_order end ) as tt_order_hol_jul,
sum(case when month_of_the_year_num = 8 then total_order end ) as tt_order_hol_aug,
sum(case when month_of_the_year_num = 9 then total_order end )as tt_order_hol_sep,
sum(case when month_of_the_year_num = 10 then total_order end)  as tt_order_hol_oct,
sum(case when month_of_the_year_num = 11 then total_order end)  as tt_order_hol_nov,
sum(case when month_of_the_year_num = 12 then total_order end ) as tt_order_hol_dec
from total_orders
group by 1

```
</details>

**agg_public_holiday table**

| "ingestion_date" | "tt_order_hol_jan" | "tt_order_hol_feb" | "tt_order_hol_mar" | "tt_order_hol_apr" | "tt_order_hol_may" | "tt_order_hol_jun" | "tt_order_hol_jul" | "tt_order_hol_aug" | "tt_order_hol_sep" | "tt_order_hol_oct" | "tt_order_hol_nov" | "tt_order_hol_dec" |
|------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|
| "2023-06-28"     | 100                | 80                 | 80                 | 85                 | 95                 | 80                 | 95                 | 85                 | 50                 | 50                 | 40                 | 40                 |

Another insight that is import to the business stakeholder is the number of late shipments and the number undelivered shipments, for this I implemented the `agg_shipments` table.


Before implemnting `agg_shipments` I needed to create `stg_shipment_performance` that would be used to determine whether the order was late, early or undelivered by creating a new column. This table would be helpful in other insights as well.

<details>
  <summary>click to view stg_shipment_performance code</summary>

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
        product_id,
		(sd.shipment_date - o.order_date) as late_delivery_date_difference,
		cast('2022-09-06' as date) -  o.order_date as undelivered_date_difference
	from shipments sd
	left join orders o on o.order_id = sd.order_id
)
select 
    order_id,
    product_id,
    case 
        when (late_delivery_date_difference >= 6) and (delivery_date is null) then 'late' 
        when (late_delivery_date_difference < 6) and (delivery_date is not null) then 'early'
        when (delivery_date is null and shipment_date is null) and (undelivered_date_difference > 15) then 'undelivered'
    end as late_early_undelivered
from date_difference

```
	
```sql
with shipment_performance as (
	select * from {{ref ('stg_shipment_performance')}}
)
select 
	cast(now() as date) as ingestion_date,
	count(case when late_early_undelivered = 'late' then True end) as tt_late_shipments,
	count(case when late_early_undelivered = 'undelivered' then True end) as tt_undelivered_shipmnets
from shipment_performance
```
</details>

<details>
  <summary>click to view agg_shipments table code</summary>	
	
```sql
with shipment_performance as (
	select * from {{ref ('stg_shipment_performance')}}
)
select 
	cast(now() as date) as ingestion_date,
	count(case when late_early_undelivered = 'late' then True end) as tt_late_shipments,
	count(case when late_early_undelivered = 'undelivered' then True end) as tt_undelivered_shipmnets
from shipment_performance
```
</details>

**agg_shipments table**
| "ingestion_date" | "tt_late_shipments" | "tt_undelivered_shipmnets" |
|------------------|---------------------|----------------------------|
| "2023-06-28"     | 175                 | 6586                       |


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
shipments_performance as (
	select 
		*
	from {{ref ('stg_shipment_performance')}}
)
,total_reviews as(
	select 
		product_id, 
		sum(review) as total_reviews, 
		rank() over(order by sum(review) desc ) as ranking
	from reviews
	group by 1	 
 ),get_orders as (
	select 
		o.product_id,
		o.order_date,
		tr.total_reviews,
		count(o.order_id) as order_count,
		rank() over(order by count(o.order_id) desc) as ranking
	from orders o
	left join total_reviews tr on tr.product_id = o.product_id
	where tr.ranking = 1
	group by 1,2,3
 ), get_late_and_early as (
	select 
		g.*,
		count(case when late_early_undelivered = 'late' then true end) as count_late,
		count(case when late_early_undelivered = 'early' then true end) as count_early
	from get_orders g
	left join shipments_performance sp on sp.product_id = g.product_id
	where ranking = 1
	group by 1,2,3,4,5
 ), total_product_reviews as (
	select	
		gle.product_id,
		sum(review) as total_product_reviews
	from reviews as r
	left join orders as o on r.product_id = o.product_id
	left join get_late_and_early as gle on gle.order_date = o.order_date
	where o.product_id = gle.product_id and o.order_date = gle.order_date
	group by 1
 ), base_table as (
	select 
	gle.*,
	total_product_reviews
	from get_late_and_early gle
	left join total_product_reviews as tpr on tpr.product_id = gle.product_id
 ), is_public_holiday as (
	select
		product_id,
		order_date,
		case when (day_of_the_week_num between 1 and 5) and work_day = false then True else False end as is_public_holiday,
		total_reviews,
		(total_reviews * 100) / (total_reviews + total_product_reviews) as pct_dist_ttl_review_points,
		(count_early * 100) / (count_early + count_late) as pct_dist_early_to_late_shipments
	from base_table bs 
	left join dim_dates as d on d.calender_dt = bs.order_date
 )
 select* from is_public_holiday	

```	
</details>

**agg_best_performing product table**

| "ingestion_date" | "product_id" | "order_date" | "is_public_holiday" | "total_reviews" | "pct_dist_ttl_review_points" | "pct_dist_early_to_late_shipments" |
|------------------|--------------|--------------|---------------------|-----------------|------------------------------|------------------------------------|
| "2023-06-29"     | "22"         | "2022-01-06" | false               | 967             | 20                           | 93                                 |

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



