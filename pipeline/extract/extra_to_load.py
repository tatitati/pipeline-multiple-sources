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

#
# extract
#
print("reads parquet:")
readsParquet=app.read.parquet("./data/trends.parquet")
readsParquetWithAggregates = readsParquet\
    .withColumn("etl_created_at", lit(datetime.datetime.now()))\
    .withColumn("etl_source", lit("data/trends.parquet"))
print(readsParquetWithAggregates.count()) # 9265028
readsParquetWithAggregates.printSchema()
# root
#  |-- datetime: timestamp (nullable = true)
#  |-- readers: long (nullable = true)
#  |-- text_id: string (nullable = true)
#  |-- __index_level_0__: long (nullable = true)
readsParquetWithAggregates.show()
# +-------------------+-------+--------------------+-----------------+
# |           datetime|readers|             text_id|__index_level_0__|
# +-------------------+-------+--------------------+-----------------+
# |2010-10-06 18:00:00|  27386|4bbb01fa3d900f59a...|             2584|
# |2010-11-16 05:00:00|  27107|2bc5b577a3855c2a0...|             3556|
# |2010-11-08 20:00:00|   8355|be1067afe57829dc4...|             3379|
# |2010-10-29 07:00:00|  16161|f4f3bb780d978465e...|             3125|

print("texts *.json:")
textsJson = app.read.option("multiline", "true").json("./data/texts/*.json")
textsJsonWithAggregates = textsJson\
    .withColumn("etl_created_at", lit(datetime.datetime.now()))\
    .withColumn("etl_source", lit("data/texts/*.json"))
print(textsJsonWithAggregates.count()) # 2286
textsJsonWithAggregates.printSchema()
# root
#  |-- id: string (nullable = true)
#  |-- text: string (nullable = true)
textsJsonWithAggregates.show()
# +--------------------+--------------------+
# |                  id|                text|
# +--------------------+--------------------+
# |5eb00135c82a5cf24...|      King Joffre...|
# |26b627a5ec8559d5e...| Through the fema...|
# |cbd110d015b008674...|    As it happens...|
# |8701687df655f8280...|  Aye. Safer apar...|
# |88f37a010b6552922...|  He made his way...|
# |92f6cffc2277f2134...| She was naked, c...|
# |66cbf13be88d3aa7a...| Sylwa Paege, —Wh...|

print("entities txt:")
entitiesTxt = app.read.text("./data/entities.txt")
entitiesWithAggregates = entitiesTxt\
    .withColumnRenamed("value", "name")\
    .withColumn("etl_created_at", lit(datetime.datetime.now()))\
    .withColumn("etl_source", lit("data/entities.txt"))
print(entitiesWithAggregates.count()) # 602
entitiesWithAggregates.printSchema()
# root
#  |-- name: string (nullable = true)
#  |-- created_at: timestamp (nullable = false)
#  |-- source: string (nullable = false)
entitiesWithAggregates.show()
# +----------+--------------------+-----------------+
# |      name|          created_at|           source|
# +----------+--------------------+-----------------+
# |       Jon|2021-11-15 13:41:...|data/entities.txt|
# |    Tyrion|2021-11-15 13:41:...|data/entities.txt|
# |      Arya|2021-11-15 13:41:...|data/entities.txt|
# |   Catelyn|2021-11-15 13:41:...|data/entities.txt|

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

savings = [
    #[ df , table ]
    [entitiesWithAggregates, "entities_current_load"],
    [textsJsonWithAggregates, "texts_current_load"],
    [readsParquetWithAggregates, "reads_current_load"],
]

for saving in savings:
    if saving[0].count() > 0:
        saving[0].write\
            .format(SNOWFLAKE_SOURCE_NAME)\
            .options(**sfOptions)\
            .option("dbtable", saving[1])\
            .mode("overwrite")\
            .save()