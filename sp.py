from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Create spark configuration object
conf = SparkConf()
conf.setMaster("local").setAppName("My app")

# Create spark context and sparksession
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

# read csv file in a dataframe
df = spark.read.csv(path="/home/manish/Downloads/VM.xlsx", header=True, sep=",")

# set variable to be used to connect the database
database = "TestDB"
table = "dbo.tbl_spark_df"
user = "test"
password = "*****"

# write the dataframe into a sql table
df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://localhost:1433;databaseName={database};") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", ) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()