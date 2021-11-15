#!/usr/local/bin/python3

from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from io import BytesIO
import boto3
from pyspark.sql import SparkSession
from pyspark import SparkContext
import configparser
import datetime
import os
import snowflake.connector
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, TimestampType, StringType
from snowflake.connector import ProgrammingError

jarPath='/Users/tati/lab/de/pipeline-user-orders/jars'
jars = [
    # spark-mysql
    f'{jarPath}/spark-mysql/mysql-connector-java-8.0.12.jar',
    # spark-snowflake
    f'{jarPath}/spark-snowflake/snowflake-jdbc-3.13.10.jar',
    f'{jarPath}/spark-snowflake/spark-snowflake_2.12-2.9.2-spark_3.1.jar', # scala 2.12 + pyspark 3.1
]
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {",".join(jars)}  pyspark-shell'

context = SparkContext(master="local[*]", appName="readJSON")
app = SparkSession.builder.appName("myapp").getOrCreate()


parser = configparser.ConfigParser()
parser.read("../pipeline.conf")
snowflake_username = parser.get("snowflake_credentials", "username")
snowflake_password = parser.get("snowflake_credentials", "password")
snowflake_account_name = parser.get("snowflake_credentials", "account_name")
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = {
    "sfURL": f'{snowflake_account_name}.snowflakecomputing.com/',
    "sfUser": snowflake_username,
    "sfPassword": snowflake_password,
    "sfDatabase": "books",
    "sfSchema": "bronze",
    "sfWarehouse": "COMPUTE_WH",
    "parallelism": "64"
}

snow_conn = snowflake.connector.connect(
    user = snowflake_username,
    password = snowflake_password,
    account = snowflake_account_name,
    database="books",
    schema="silver")

# check if previous load exist:


tables = [
    ['texts_enriched', 'texts_stream'],
]

# prepare tables and streams
for table in tables:
    print("creating table")
    create_table_sql = f'create table if not exists BOOKS.silver.{table[1]} like BOOKS.SILVER.{table[0]};'
    cur = snow_conn.cursor()
    cur.execute(create_table_sql)

    print("creating stream for table")
    create_stream_sql = f'CREATE STREAM if not exists stream_{table[1]} ON TABLE BOOKS.SILVER.{table[1]};'
    cur = snow_conn.cursor()
    cur.execute(create_stream_sql)

    print("merging into table with stream")
    merge_sql = """
            merge into BOOKS.silver.TEXTS_STREAM t_stream
                using(
                    select
                        ID,
                        TEXT,
                        ETL_CREATED_AT,
                        ETL_SOURCE, 
                        UNIVERSO_LITERARIO
                    from BOOKS.SILVER.TEXTS_ENRICHED
                ) t_dedups
                on t_stream.id = t_dedups.id
                when matched then
                    update set 
                        t_stream.UNIVERSO_LITERARIO = t_dedups.UNIVERSO_LITERARIO
                when not matched then
                    insert(id, text, etl_created_at, etl_source, UNIVERSO_LITERARIO)
                    values(t_dedups.ID, t_dedups.TEXT, t_dedups.ETL_CREATED_AT, t_dedups.ETL_SOURCE, t_dedups.UNIVERSO_LITERARIO);
        
    """
    cur = snow_conn.cursor()
    cur.execute("BEGIN;")
    cur.execute(merge_sql)
    cur.execute("COMMIT;")

cur.close()



