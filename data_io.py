import psycopg2
conn = psycopg2.connect(''' host=localhost dbname=regligare_db user=postgres,password=Manish12 ''')
cur = conn.cursor()
insert_query = " select * from 	cmdb.extraction_info"
cur.execute(insert_query)
conn.commit()

# def write_db(df):
#     df.write.mode('overwrite') \
#     .format("jdbc") \
#     .option("url", f"jdbc:postgresql://localhost:5432/postgres") \
#     .option("dbtable", "ttable") \
#     .option("user", "postgres") \
#     .option("password", "Manish12") \
#     .option("driver", "org.postgresql.Driver") \
#     .save()