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

snow_conn = snowflake.connector.connect(
    user = snowflake_username,
    password = snowflake_password,
    account = snowflake_account_name,
    database="books",
    schema="bronze")
cur = snow_conn.cursor()

tables = [
    ['entities_current_load', 'entities_previous_load', 'entities_dedup'],
    ['texts_current_load',    'texts_previous_load',    'texts_dedup'],
    ['reads_current_load',    'reads_previous_load',    'reads_dedup']]

for listTables in tables:
    cur.execute(f'create table if not exists BOOKS.BRONZE.{listTables[1]} like {listTables[0]};') # create "previous" table if needed
    cur.execute(f'create table if not exists BOOKS.SILVER.{listTables[2]} like {listTables[0]};') # create "dedup" table if needed

    # Current-Previous = Dedup
    cur.execute(f'truncate table BOOKS.BRONZE.{listTables[2]};')  # truncate "dedup" table
    cur.execute("""
        insert into BOOKS.SILVER.%s
            select * from BOOKS.BRONZE.%s
            minus
            select * from BOOKS.BRONZE.%s
    """ % (listTables[2], listTables[0], listTables[1]))

# swap current-previous
for table in tables:
    cur.execute(f'alter table BOOKS.BRONZE.{table[1]} swap with {table[0]};') # swap current-previous
    cur.execute(f'truncate table BOOKS.BRONZE.{table[0]};') # truncate current

cur.close()