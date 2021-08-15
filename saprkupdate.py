#from pip._vendor.requests import packages
import lit as lit
#from pip._vendor.pyparsing import col
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession

#pyspark --packages com.crealytics:spark-excel_2.11:0.11.1
appName = "PySpark SQL Server date-time transformations"
master = "local"
#creating spark session
spark = SparkSession\
          .builder \
          .appName("Python Spark SQL basic example") \
          .config("spark.some.config.option", "some-value") \
          .getOrCreate()

#reading xlsx file
import pandas as pd
import xlrd
v1 = pd.read_excel("/home/bhawana/Desktop/vendor/VM.xlsx")
v1DF = sqlContext.createDataFrame(v1)
#v1DF.show()
#v1DF.printSchema()
v2 = pd.read_excel("/home/bhawana/Desktop/vendor/VM2.xlsx")
v2DF = sqlContext.createDataFrame(v2)
#v2DF.show()
#########################################################################################
from pyspark.sql.functions import lit, date_sub, current_date
v2_inc=v2DF.withColumnRenamed('Vendor Code','Vendor_Code') \
                .withColumnRenamed('Vendor Name','Vendor_Name') \
                .withColumnRenamed('Region Name','Region_Name') \
                .withColumnRenamed('City Name','City_Name') \
                .withColumnRenamed('Contact No','Contact_No') \
               .withColumnRenamed('Created Date','Created_Date')\
               .withColumn("eft_start_date",lit("NULL"))\
               .withColumn("eft_end_date",lit("NULL"))\

v2_inc=v2_inc.select("Vendor_Code","Vendor_Name","Plant ","Region ","Region_Name","City_Name","Contact_no","Created_Date","eft_start_date","eft_end_date","Updated_date","is_active")
#print("v2_inc")
#v2_inc.printSchema()
v2_inc.show()
from pyspark.sql.functions import lit, date_sub, current_date

v1_history=v1DF.withColumnRenamed('Vendor Code','Vendor_Code') \
                .withColumnRenamed('Region Name','Region_Name') \
                .withColumnRenamed('City name','City_Name') \
                .withColumnRenamed('Vendor Name','Vendor_Name') \
                .withColumn("update_date",lit("Null"))
#print("v1_histroy")
#v1_history.printSchema()
#v1_history.show(52)
#v2=v1_history.where((col("is_active")==0))
vfilter=v1_history\
     .alias('hist')\
     .filter(v1_history.is_active==0)\
                  .select('hist.Vendor_Code',
                        'hist.Vendor_Name',
                        'hist.Plant ',
                        'hist.Region ',
                        'hist.Region_Name',
                        'hist.City_Name',
                        'hist.Contact_no',
                        'hist.Created_Date',
                        'hist.eft_start_date',
                        'hist.eft_end_date')\
                .withColumn('update_date',lit('null')) \
                .withColumn("is_active",lit("0"))
vfilter.show()

v2filter=v1_history.filter(v1_history.is_active==1)\
                   .alias("hist")\
                   .select('hist.Vendor_Code',
                        'hist.Vendor_Name',
                        'hist.Plant ',
                        'hist.Region ',
                        'hist.Region_Name',
                        'hist.City_Name',
                        'hist.Contact_no',
                        'hist.Created_Date',
                        'hist.eft_start_date',
                        'hist.eft_end_date',
                        'hist.update_date',
                        'hist.is_active')


v2filter.show(53)
##############################################
#from functools import reduce
#from pyspark.sql import DataFrame
#def unionall(*a):
#  return reduce(DataFrame.unionAll,a)
#unionall(v2filter,v2_inc).orderBy("Vendor_Code").show()
hist=v2filter.alias('hist')
inc=v2_inc.alias('inc')
inner_join=hist.join(inc , hist.Vendor_Code == inc.Vendor_Code,'left') \
                .select('hist.Vendor_Code',
                        'hist.Vendor_Name',
                        'hist.Plant ',
                        'hist.Region ',
                        'hist.Region_Name',
                        'hist.City_Name',
                        'hist.Contact_no',
                        'hist.Created_Date',
                        'hist.eft_start_date',
                col('inc.eft_end_date',date_sub(current_date(),1)))


# .withColumn('eft_end_date',current_date()-1) \
               # .columns('hist.Vendor_Name')
# hist.printSchema()
# inner_join.printSchema()
inner_join.show()

#df=v2filter.union(v2_inc).orderBy("Vendor_Code")
#df1=df.groupBy(df.Vendor_Code).count()



#df.show(60)
#df.filter(df.homeworkSubmitted == True).groupby(df.studentId).count()