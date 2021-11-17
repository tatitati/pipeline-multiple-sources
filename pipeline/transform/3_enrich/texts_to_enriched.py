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
    "sfWarehouse": "COMPUTE_WH",
    "parallelism": "64"
}

# add universon_literario
def get_prediction(text):
        LABELS = ["got", "lotr", "hp"]
        model = load('/Users/tati/lab/de/pipeline-multiple-sources/classification-service/classification_pipeline.joblib')
        class_index = model.predict([text])[0]
        return LABELS[class_index]

udf_get_prediction = udf(lambda x: get_prediction(x), StringType())

tables = ['texts_dedup', 'texts_enriched']
sfOptions['schema'] = 'silver'
texts_dedup = app.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option("query", f'select * from BOOKS.SILVER.{tables[0]}') \
            .load()
texts_enriched = texts_dedup.withColumn(
    'universo_literario',
    udf_get_prediction(texts_dedup['text'])
)


# add a column for each entity
entities_df = app.read\
    .format(SNOWFLAKE_SOURCE_NAME)\
    .options(**sfOptions)\
    .option("query", 'select name from BOOKS.SILVER.entities_enriched')\
    .load()

for index, row in entities_df.toPandas().iterrows():
    texts_enriched = texts_enriched.withColumn(row['NAME'], lit(False))


# save to enriched
sfOptions['schema'] = 'silver'
texts_enriched.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", f'texts_enriched')\
        .mode("overwrite") \
        .save()

