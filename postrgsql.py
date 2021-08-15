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

def write(df,path):
    df.repartition(1) \
   .write.mode('overwrite') \
   .save(path, format='csv', header=True)

def process(read_df):

    drop_columns_list=["CSPM_AADHAR_NO","CSPM_BRTH_DATE","CSPM_CSTREG_DATE",
                       "CSPM_CUST_FRST_NAME","CSPM_CUST_FULL_NAME",
                       "CSPM_CUST_LAST_NAME","CSPM_CUST_MIDL_NAME",
                       "CSPM_CUST_PROPT","CSPM_EMAIL_BLK_YN","CSPM_GST_NO","CSPM_INSU_COM_NAME","CSPM_INSU_CON_NO",
                       "CSPM_INSU_CUDT_ADDR","CSPM_INSU_EMAIL_NO","CSPM_INSU_GST_NO","CSPM_INSU_IS_HAP",
                       "CSPM_INSU_STCD_CODE","CSPM_LSTREG_DATE","CSPM_LSTREG_NO","CSPM_MARRI_DATE",
                       "CSPM_PAN_NO","CSPM_REGIST_YN","CSPM_RESDC_PHONE_NO","CSPM_RESDC_STD",
                       "CSPM_SERVICE_DISCOUNT","CSPM_SMS_BLK_YN","CSPM_SVC_LABR_DSCNT_RATE","CSPM_SVC_PART_DSCNT_RATE",
                       "CSPM_UPDT_DTIME","CSPM_WHATA_ID","DRIVE_LICENSE_NO","FILE_DOC_NO","PASS_PORT_NO","SALUT_CODE"]


    new_list=read_df.withColumn('Cust_Link_Key', sf.concat(sf.col('CSPM_DLR_NO'), sf.col('CSPM_CUST_NO')).cast(StringType()))\
         .withColumn("Created_Date", sf.to_timestamp(sf.lit("2020-01-01"))) \
        .withColumn("eft_start_date", sf.current_timestamp())\
        .withColumn("End_Date", sf.lit("1980-01-01").cast(TimestampType())) \
        .withColumn("Update_date", sf.to_timestamp(sf.lit(None))) \
        .withColumn("is_active", sf.lit("1")) \
        .drop(*drop_columns_list).drop(*drop_columns_list)
    new_list=new_list.select("Cust_Link_Key", 'CSPM_CMPN_NO', 'CSPM_CORP_NO', 'CSPM_DLR_NO', 'CSPM_CUST_NO', 'CSPM_CHRG_SALES_EMP_NO',
            'CSPM_CHRG_MNGER_EMP_NO', 'CSPM_CUST_TYPE', 'CSPM_EMAIL', 'CSPM_CMPN_TEL_NO', 'CSPM_CRTE_DTIME',
            'CSPM_CRTE_EMP_NO', 'CSPM_UPDT_EMP_NO', 'CSPM_CSTREG_NO', 'AADHAR_VRFI', 'M_MOBL_PHONE_NO',
            'S_MOBL_PHONE_NO', 'CSPM_CUST_ID', 'CSPM_DLR_CODE', 'Created_Date', 'eft_start_date',
            'Update_date', 'End_Date', 'is_active')
    return new_list

read_df=read_csv("/home/manish/Downloads/kia_source/CMM_CADLRM_TB.csv")
# database="KIA"
output_df=process(read_df)
output_df.write.mode('overwrite') \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "FullLoad") \
    .option("user", "") \
    .option("password", "") \
    .option("driver", "org.postgresql.Driver") \
    .save()

#write(output_df,"/home/prashant/Downloads/kia_destination/Fulload/CRM_CDCSPM_TB")