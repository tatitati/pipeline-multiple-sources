#!/usr/local/bin/python3

from pyspark.sql.functions import trim
import pymysql
from pyspark.sql import SparkSession

# start an spark session
# =================================
app = SparkSession.builder.appName("myapp").getOrCreate()

# extract
# =======
reads=app.read.parquet("../data/trends.parquet")
texts = app.read.option("multiline","true").json("../data/texts/*.json")
entities = app.read.text("../data/entities.txt")


# Transform
# =======
entitiesTrim = entities\
    .withColumn("name", trim(entities.value))\
    .drop("value")
entitiesTrim.show()

# load
# =======
db = pymysql.connect(host='localhost',
                             user='root',
                             password='admin',
                             database='seedtagdb',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = db.cursor()

for value in entitiesTrim.collect():
    q = """insert into entities(name) values (%s);"""
    val = (value)
    cursor.execute(q, val)
db.commit()

# cursor.execute("""select * from entities;""")
# for result in cursor.fetchall():
#     print(result)
# db.close()