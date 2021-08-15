from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
import snowflake

spark = SparkSession \
.builder \
.config("spark.jars","/home/manish/Downloads/snowflake-jdbc-3.12.13.jar,/home/manish/Downloads/spark-snowflake_2.12-2.8.2-spark_3.0.jar") \
.config("spark.sql.catalogImplementation", "in-memory") \
.getOrCreate()

import os

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

SNOWFLAKE_OPTIONS = {
    'sfURL': os.environ.get("SNOWFLAKE_URL", "c.ap-south-1.aws.snowflakecomputing.com"),
    'sfUser': os.environ.get("SNOWFLAKE_USER", ""),
    'sfPassword': os.environ.get("SNOWFLAKE_PASSWORD", ""),
    'sfDatabase': os.environ.get("SNOWFLAKE_DATABASE", "QUICKSTART"),
    'sfSchema': os.environ.get("SNOWFLAKE_SCHEMA", "Public"),
    'sfWarehouse': os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
     "sfRole": os.environ.get("SYSADMIN")
    }

# sc = SparkContext("local", "Simple App")
# spark = SQLContext(sc)
# spark_conf = SparkConf().setMaster('local').setAppName('snowflake').set("spark.driver.extraClassPath","/home/manish/Downloads/snowflake-jdbc-3.12.13.jar")\
# .set("spark.driver.extraClassPath","/home/manish/Downloads/spark-snowflake_2.12-2.8.2-spark_3.0.jar")\

# Set options below
# sfOptions = {
#    "sfURL" : "nv10563.ap-south-1.aws.snowflakecomputing.com",
#     "sfUser":"MANISH3737",
#     "sfPassword" :"Manish@1970",
#      "sfSchema" : "Public",
#     "sfDatabase" :"SALESDB",
#     "sfWarehouse" :"COMPUTE_WH"
# }

df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**SNOWFLAKE_OPTIONS) \
  .option('dbtable',"TRANS_RECORDS")\
    .load()


df.show()