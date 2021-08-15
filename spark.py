from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark import SparkContext, SparkConf, SQLContext
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, date_sub, current_date

class fullload:
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    df = spark.read.format('com.crealytics.spark.excel') \
        .options(header=True, inferSchema=True) \
        .load("/home/manish/Downloads/VM.xlsx")
    df.show()
    # Want to change the name of coumn in the target folder where the target file is loaded
    # a = df.withColumnRenamed('Employee Name', 'Name') \
    #       .withColumnRenamed("City","Current City")
    # a.show()
    # a.repartition(1) \
    #     .write.mode('overwrite') \
    #     .save(path="/home/prashant/Desktop/ETL/target", format='csv', header=True)