from data_io import *

def read_postgres_db():

    jdbc_read = spark.read.format("jdbc") \
         .option("url", "jdbc:postgresql:postgres") \
         .option("dbtable", "hdfs") \
         .option("user", "") \
         .option("password", "") \
         .option("driver", "org.postgresql.Driver") \
        .load()
    return jdbc_read

if __name__ == '__main__':
    abc=read_postgres_db()
    write(abc,"hdfs://localhost:9000/manis.csv")