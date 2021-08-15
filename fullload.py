from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark import SparkContext, SparkConf, SQLContext
import pandas as pd
import xlrd
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, date_sub, current_date

table = "FullLoad"
user = "SA"
password = "Prashant@12"
database = "TestDB"


class fullload:
    appName = "PySpark SQL Server Example - via JDBC"
    master = "local"
    conf = SparkConf() \
        .setAppName(appName) \
        .setMaster(master) \
        .set("spark.driver.extraClassPath",
             "/home/manish/Downloads/sqljdbc_4.2.8112.200_enu/sqljdbc_4.2/enu/jre8")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession
    df = pd.read_excel("/home/manish/Downloads/VendorMaster1.xlsx")
    df1 = spark.createDataFrame(df).withColumn("is_active", lit("c"))
    df1.show()
    """sf = df1.select([col(c).cast("string") for c in df1.columns])
    sf.printSchema()

    sf.write.mode('overwrite') \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://localhost:1433;databaseName={database};") \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()"""