from pyclbr import Class

import findspark
import lit as lit
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import col,current_date,date_sub
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, desc


appName = "PySpark SQL Server Example - via JDBC"
master = "local"
conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master) \
    .set("spark.driver.extraClassPath",
         "/home/bhawana/Downloads/sqljdbc_4.2.8112.200_enu/sqljdbc_4.2/enu/jre8/sqljdbc42.jar")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession
##########################################################################################
#set variable to be used to connect the database
database = "bhawana"
table = "dbo.target"
user = "SA"
password = "Patel@@12"
jdbcdriver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcUrl= f"jdbc:sqlserver://localhost;databaseName={database};"
#####################################################################

properties = {
  "database":"bhawana",
  "table":"dbo.target",
  "user":"SA",
  "password":"Patel@@12",
  "jdbcdriver":"com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "jdbcUrl": f"jdbc:sqlserver://localhost;databaseName=bhawana;"
}
sc=SparkContext("local","Broadcast")
brconnect=sc.broadcast(properties)
print(brconnect.value)


##################################################################################
import pandas as pd
v2 = pd.read_excel("/home/bhawana/Desktop/vendor/VM2.xlsx")
v2DF=spark.createDataFrame(v2)
inc=v2DF.withColumnRenamed('Vendor Code','Vendor_Code') \
                .withColumnRenamed('Vendor Name','Vendor_Name') \
                .withColumnRenamed('Region Name','Region_Name') \
                .withColumnRenamed('City Name','City_Name') \
                .withColumnRenamed('Contact No','Contact_no') \
                .withColumnRenamed('Created Date','Created_Date')\
                .withColumn('eft_start_date',lit('NULL'))\
                .withColumn('eft_end_date',lit('00/00/0000'))\
                .select('Vendor_Code','Vendor_Name','Plant','Region',
                        'Region_Name','City_Name','Contact_no',
                        'Created_date','eft_start_date','eft_end_date',
                        'updated_date','is_active')

#print("v2_inc")
inc2=inc.select([col(c).cast("string") for c in inc.columns])
# inc2.printSchema()
#####################################################################
inc2.coalesce(10).foreachPartition(
    partition={
               properties:brconnect.value,

              "user":properties.get("user"),
              "password": properties.get("@@12"),
              "jdbcdriver": properties.get("com.microsoft.sqlserver.jdbc.SQLServerDriver"),

               # Class.forName("jdbcdriver").newInstance(),
               # dbConn = DriverManager.getConnection("jdbcUrl")

    })
#######################################################################33
sqlDF = spark.read.format("jdbc") \
   .option("url", f"jdbc:sqlserver://localhost:1433;databaseName={database}") \
   .option("dbtable", table) \
   .option("user", user) \
   .option("password", password) \
   .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
   .load()

sqlDF1=sqlDF.select([col(c).cast("string") for c in sqlDF.columns])
primary_keys = ['Vendor_Code']

##########################################################################
# inc2.write.mode('append') \
#     .format("jdbc") \
#     .option("url", f"jdbc:sqlserver://localhost;databaseName={database};") \
#     .option("dbtable", table) \
#     .option("user", user) \
#     .option("password", password) \
#     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
#     .save()