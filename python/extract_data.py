# Code to extract data from s3 bucket to postgres. 
import boto3
import psycopg2
import pandas as pd
import os

from botocore import UNSIGNED
from botocore.client import Config
from config import host, port, username, password, database
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
  database=database,
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
