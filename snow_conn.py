from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext

sc = SparkContext("local", "App")
spark = SQLContext(sc)
spark_conf = SparkConf().setMaster('local').setAppName('test')
sc=SparkContext.getOrCreate()

sfOptions = {
    "sfURL": "nv.ap-south-1.aws.snowflakecomputing.com",
    "sfUser": "",
    "sfPassword": "@1970",
    "sfDatabase": "SALESDB",
    "sfSchema": "Public",
    "sfWarehouse": "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df=spark.read.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("query","SELECT * FROM EMP_DETAILS") \
    .load()
df.show()

