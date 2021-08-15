import subprocess
from datetime import datetime
# import mysql.connector
# List of scripts to be executed
import pyodbc

list_scripts = ["CRM_SCEQCR_TB","CRM_SCEQFU_TB","CRM_SCEQRY_TB"]

# Setting up sql connection
configurations = {
    "database": "kia",
    "user": "",
    "password": "",
    "driver": "/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.5.so.2.1",
    "server": "localhost"
}
mydb = pyodbc.connect('Driver=' + configurations["driver"] +
                          ';SERVER=' + configurations["server"] +
                          ';DATABASE=' + configurations["database"] +
                          ';UID=' + configurations["user"] +
                          ';PWD=' + configurations["password"])
mycursor = mydb.cursor()

# Running each script through a loop
for scrip in list_scripts:
    start = datetime.now()

    #  Executing Script
    try:
        output = subprocess.check_output(['sh', '/home/manish/Desktop/script.sh', scrip])
    except subprocess.CalledProcessError as exc:
        output = ""
        status = "FAILED"
    else:
        output = output.decode("utf-8")
        status = "COMPLETED"

    end = datetime.now()
    print(status)
    print(output)
    print(start)
    print(end)

    # Saving the data to sql database
    sql = "INSERT INTO DETAILS() VALUES (%s,%s,%s,%s,%s,%s)"
    val = (scrip, start, end, (end - start), status, output)
    mycursor.execute(sql,val)

mydb.commit()