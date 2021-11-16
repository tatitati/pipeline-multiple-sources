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

jarPath = '/Users/tati/lab/de/pipeline-user-orders/jars'
jars = [
    # spark-mysql
    f'{jarPath}/spark-mysql/mysql-connector-java-8.0.12.jar',
    # spark-snowflake
    f'{jarPath}/spark-snowflake/snowflake-jdbc-3.13.10.jar',
    f'{jarPath}/spark-snowflake/spark-snowflake_2.12-2.9.2-spark_3.1.jar',  # scala 2.12 + pyspark 3.1
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
    "sfSchema": "silver",
    "sfWarehouse": "COMPUTE_WH",
    "parallelism": "64"
}

snow_conn = snowflake.connector.connect(
    user=snowflake_username,
    password=snowflake_password,
    account=snowflake_account_name,
    database="books",
    schema="silver")

cur = snow_conn.cursor()

tables = ['entities_enriched', 'dim_entities']

entities_df = app.read\
    .format(SNOWFLAKE_SOURCE_NAME)\
    .options(**sfOptions)\
    .option("query", 'select name from BOOKS.SILVER.entities_enriched')\
    .load()

print(entities_df.toPandas().transpose())
# transposed = entities_df.T
# transposed.show()
#
# cur.execute(
#     """
#     create table if not exists BOOKS.GOLD.dim_entitie(
#         sk number not null autoincrement primary key,
#
#     );
#     """
# )
#
# cur.close()
#
#

