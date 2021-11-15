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
    schema="bronze")

# check if previous load exist:


tables = [
    ['entities_current_load', 'entities_previous_load', 'entities_dedup'],
    ['texts_current_load', 'texts_previous_load', 'texts_dedup'],
    ['reads_current_load', 'reads_previous_load', 'reads_dedup'],
]


for listTables in tables:
    x_load_current = app.read.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptions) \
                .option("query", f'select * from {listTables[0]}') \
                .load()

    x_load_previous = app.createDataFrame([], x_load_current.schema)

    previous_sql = """
        SELECT count(*)
        FROM information_schema.tables
        where 
            table_schema='BRONZE'
            and table_name like '%s';
    """
    cur = snow_conn.cursor()
    cur.execute(previous_sql % (listTables[1]))
    for (col1) in cur:
        if col1[0] > 0:
            x_load_previous = app.read.format(SNOWFLAKE_SOURCE_NAME) \
                .options(**sfOptions) \
                .option("query", f'select * from {listTables[1]}') \
                .load()
            break

    x_dedup = x_load_current.subtract(x_load_previous)
    x_dedup.show()

    x_dedup.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", f'{listTables[2]}')\
        .mode("append") \
        .save()