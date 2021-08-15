from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import functions as sf
from pyspark.sql.types import TimestampType, DateType, StringType

appName = "PySpark SQL Server Example - via JDBC"
master = "local"
conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master) \
    .set("spark.driver.extraClassPath",
         "/home/manish/Downloads/postgresql-42.2.16.jar")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession

def read_csv(path):

    df_read = spark.read.format('csv') \
            .options(header=True, inferSchema=True) \
            .load(path)
    return df_read

def process(read_df):

    new_list=read_df.withColumn("Created_Date", sf.to_timestamp(sf.lit("2020-01-01"))) \
        .withColumn("eft_start_date", sf.current_timestamp())\
        .withColumn("End_Date", sf.lit("1980-01-01").cast(TimestampType())) \
        .withColumn("Update_date", sf.to_timestamp(sf.lit(None))) \
        .withColumn("is_active", sf.lit("1"))

    return new_list


read_df=read_csv("hdfs://localhost:9000/ModelMaster.csv")

output_df=process(read_df)
output_df.write.mode('overwrite') \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "hdfs") \
    .option("user", "") \
    .option("password", "") \
    .option("driver", "org.postgresql.Driver") \
    .save()