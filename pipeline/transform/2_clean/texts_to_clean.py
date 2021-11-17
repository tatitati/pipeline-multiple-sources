#!/usr/local/bin/python3

from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from io import BytesIO
import boto3
from pyspark.sql import SparkSession
from pyspark import SparkContext
import configparser
import datetime
from pyspark.sql.functions import udf
import os
import snowflake.connector
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, TimestampType, StringType
from snowflake.connector import ProgrammingError
from joblib import load

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
sfOptions = {
    "sfURL": f'{snowflake_account_name}.snowflakecomputing.com/',
    "sfUser": snowflake_username,
    "sfPassword": snowflake_password,
    "sfDatabase": "books",
    "sfWarehouse": "COMPUTE_WH",
    "parallelism": "64",
    "schema": "silver"
}

tables = ['texts_dedup', 'texts_clean']

def trim_texts(text):
        return text.strip()
udf_trim_texts = udf(lambda x: trim_texts(x), StringType())

texts_dedup = app.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option("query", f'select * from BOOKS.SILVER.{tables[0]}') \
            .load()

entities_dedup_cleaned = texts_dedup\
    .withColumn(
        'text_cleaned',
        udf_trim_texts(texts_dedup['text']))\
    .drop_duplicates(["text_cleaned"])\
    .drop('text')\
    .withColumnRenamed('text_cleaned', 'text')

entities_dedup_cleaned.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", f'text_clean')\
        .mode("overwrite") \
        .save()

