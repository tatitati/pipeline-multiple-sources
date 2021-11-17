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
    user=snowflake_username,
    password=snowflake_password,
    account=snowflake_account_name,
    database="books",
    schema="silver")

tables = [
    ['texts_enriched', 'texts_stream'],
]

cur = snow_conn.cursor()

# prepare tables and streams
for table in tables:
    cur.execute(f'create table if not exists BOOKS.silver.{table[1]} like BOOKS.SILVER.{table[0]};') # creating table "texts_stream" it not exist
    cur.execute(f'CREATE STREAM if not exists stream_{table[1]} ON TABLE BOOKS.SILVER.{table[1]};') # creating stream for table "texts_stream" if not exists

    # merging into table with stream
    cur.execute("BEGIN;")
    cur.execute("""
        merge into BOOKS.SILVER.texts_stream t_stream
            using(
                select
                    ID,
                    TEXT,
                    ETL_CREATED_AT,
                    ETL_SOURCE, 
                    UNIVERSO_LITERARIO
                from BOOKS.SILVER.TEXTS_ENRICHED
            ) t_enriched
            on t_stream.id = t_enriched.id
            when matched then
                update set 
                    t_stream.UNIVERSO_LITERARIO = t_enriched.UNIVERSO_LITERARIO
            when not matched then
                insert(id, text, etl_created_at, etl_source, UNIVERSO_LITERARIO)
                values(t_enriched.ID, t_enriched.TEXT, t_enriched.ETL_CREATED_AT, t_enriched.ETL_SOURCE, t_enriched.UNIVERSO_LITERARIO);
        
    """)
    cur.execute("COMMIT;")

cur.close()
