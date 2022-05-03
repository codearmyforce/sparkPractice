'''
Reading data from local mysql and wrting back after aggregation operations.

@author:Mr. Ravi Kumar
'''
#creating the spark session

from os import truncate


def create_session():
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.appName("mysql").getOrCreate()
    sc=spark.sparkContext
    return spark, sc

#mysql connection details

driver = "com.mysql.jdbc.Driver"
url = "jdbc:mysql://127.0.0.1:3306/test"
user = "root"
pwd = "India@123"

#Building connection and reading data from mysql
def read_data(spark, sc):
    sourceDf = spark.read.format("jdbc").option("driver", driver)\
    .option("url", url)\
    .option("dbtable", "employee")\
    .option("user", user)\
    .option("password", pwd)\
    .load()
    print("Bulid mysql connection successfully ! ")
    return sourceDf
#validating the data
def data_disp(spark,sc):
    df=read_data(spark, sc)
    print("***************************Data Preview*******************************************")
    df.show(truncate=0)
   

#2nd highest employee Job wise
def secondHighest(spark,sc):
    import pyspark.sql.window as W
    import pyspark.sql.functions as F
    import pyspark.sql.types as T

    sourceDf=read_data(spark,sc)
    #windownspec
    v=W.Window.partitionBy(sourceDf["Job"]).orderBy(sourceDf["Salary"].desc())
    highest=sourceDf.withColumn("2nd_Highest", F.dense_rank().over(v))
    return highest
#writing back after processing 
def write_mysql(spark, sc):
    output=secondHighest(spark, sc)
    output.write.format("jdbc").option("driver", driver)\
    .option("url", url)\
    .option("dbtable", "Second_highest")\
    .option("user", user)\
    .option("password", pwd)\
    .save()


    #main function

if __name__ == '__main__':
    spark,sc=create_session()
    read_data(spark, sc)
    data_disp(spark, sc)
    secondHighest(spark,sc)
    write_mysql(spark, sc)
