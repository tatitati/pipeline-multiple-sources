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
readsParquet.show()
# +-------------------+-------+--------------------+-----------------+
# |           datetime|readers|             text_id|__index_level_0__|
# +-------------------+-------+--------------------+-----------------+
# |2010-10-06 18:00:00|  27386|4bbb01fa3d900f59a...|             2584|
# |2010-11-16 05:00:00|  27107|2bc5b577a3855c2a0...|             3556|
# |2010-11-08 20:00:00|   8355|be1067afe57829dc4...|             3379|
# |2010-10-29 07:00:00|  16161|f4f3bb780d978465e...|             3125|
# |2010-08-04 08:00:00|  21851|c900a5a1ac9da658e...|             1062|
# |2010-10-20 10:00:00|   9580|1185d97738fd4adbe...|             2912|
# |2010-09-25 02:00:00|  10663|48a83a7418bbb39b9...|             2304|
# |2010-10-27 19:00:00|  13672|c9b018528fcdc536d...|             3089|
# |2010-11-26 13:00:00|  31357|a8814b2bac37b768b...|             3804|
# |2010-11-03 01:00:00|  19826|ab3d1f60318abb2e6...|             3240|
# |2010-08-13 00:00:00|  14676|da940c549b0c26253...|             1270|
# |2010-10-19 19:00:00|  11407|74e8dddb3f3ed75d2...|             2897|
# |2010-11-19 20:00:00|  11228|c7df27c487b72a93b...|             3643|
# |2010-11-21 16:00:00|  33505|22876d559f1e30baa...|             3687|
# |2010-06-30 06:00:00|  42722|060f2f39e3ba0f37f...|              220|
# |2010-10-26 03:00:00|  14089|4c7ce49e6c6b88871...|             3049|
# |2010-07-17 04:00:00|  14020|5e6133601de14ef54...|              626|
# |2010-08-01 05:00:00|  42474|17f8f4dec2571ad21...|              987|
# |2010-11-04 02:00:00|  19269|5187cf6b9464b2141...|             3265|
# |2010-08-25 08:00:00|  20990|0942b475c7c57fd0c...|             1566|
# +-------------------+-------+--------------------+-----------------+
print("texts *.json:")
print(textsJson.count()) # 2286
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
# |c347595f946c4d132...|   Stubborn child...|
# |423ce4d67843c83be...|      The banner ...|
# |9f4b73ce2125b7889...|      The banner ...|
# |a9a51b4ec10108ea3...| Ser Flement Brax...|
# |848cc954f6b9fdbcc...|      HOUSE LANNI...|
# |5a2225066df589129...| It took him a wh...|
# |d04f57cb14da715c2...| If you sit on  t...|
# |5196b4fabb066053d...|      King Joffre...|
# |feef210c9d9e6520e...| Crouch - 535  TW...|
# |48a83a7418bbb39b9...|  Lord Frey agree...|
# |ac144bc1e321b7c0a...|.. a tame werewol...|
# |146029888f79fc7bd...|  "On my whistle,...|
# |b9be434fa1b345f37...| It was surrounde...|
# +--------------------+--------------------+
print("entities txt:")
print(entitiesTxt.count()) # 602
entitiesTxt.show()
# +----------+
# |     value|
# +----------+
# |       Jon|
# |    Tyrion|
# |      Arya|
# |   Catelyn|
# |       Ned|
# |      Robb|
# |   Joffrey|
# |     Jaime|
# |    Robert|
# |   Stannis|
# |      Dany|
# |       Sam|
# |     Renly|
# |Winterfell|
# |    Cersei|
# |     Tywin|
# |     Hodor|
# |     Sansa|
# |   Brienne|
# | Lannister|
# +----------+


# join textsJson and readsParquet
textsJson.join(readsParquet, textsJson['id'] == readsParquet['text_id']).show()
# +--------------------+--------------------+-------------------+-------+--------------------+-----------------+
# |                  id|                text|           datetime|readers|             text_id|__index_level_0__|
# +--------------------+--------------------+-------------------+-------+--------------------+-----------------+
# |31f485d6ad15f6cfb...|  When they final...|2010-11-10 02:00:00|  35111|31f485d6ad15f6cfb...|             3409|
# |31f485d6ad15f6cfb...|  When they final...|2010-07-10 19:00:00|  18426|31f485d6ad15f6cfb...|              473|
# |31f485d6ad15f6cfb...|  When they final...|2010-07-09 08:00:00|  23687|31f485d6ad15f6cfb...|              438|
# |31f485d6ad15f6cfb...|  When they final...|2010-07-16 05:00:00|  30290|31f485d6ad15f6cfb...|              603|
# |31f485d6ad15f6cfb...|  When they final...|2010-10-02 07:00:00|  49190|31f485d6ad15f6cfb...|             2477|