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

app = SparkSession.builder.appName("myapp").getOrCreate()

#
# extract
#
readsParquet=app.read.parquet("../data/trends.parquet")
textsJson = app.read.option("multiline", "true").json("../data/texts/*.json")
entitiesTxt = app.read.text("../data/entities.txt")


print("reads parquet:")
print(readsParquet.count()) # 9265028
readsParquet.printSchema()
# root
#  |-- datetime: timestamp (nullable = true)
#  |-- readers: long (nullable = true)
#  |-- text_id: string (nullable = true)
#  |-- __index_level_0__: long (nullable = true)
readsParquet.show()
# +-------------------+-------+--------------------+-----------------+
# |           datetime|readers|             text_id|__index_level_0__|
# +-------------------+-------+--------------------+-----------------+
# |2010-10-06 18:00:00|  27386|4bbb01fa3d900f59a...|             2584|
# |2010-11-16 05:00:00|  27107|2bc5b577a3855c2a0...|             3556|
# |2010-11-08 20:00:00|   8355|be1067afe57829dc4...|             3379|
# |2010-10-29 07:00:00|  16161|f4f3bb780d978465e...|             3125|

print("texts *.json:")
print(textsJson.count()) # 2286
textsJson.printSchema()
# root
#  |-- id: string (nullable = true)
#  |-- text: string (nullable = true)
textsJson.show()
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
print(entitiesTxt.count()) # 602
entitiesTxt.printSchema()
# root
#  |-- value: string (nullable = true)
entitiesTxt.show()
# +----------+
# |     value|
# +----------+
# |       Jon|
# |    Tyrion|
# |      Arya|
# |   Catelyn|
# |       Ned|


