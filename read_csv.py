'''
Scripts: Read record from csv file and load to mysql
@author: Mr. Ravi Kumar

'''
from os import truncate


def create_session():
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.appName("Read csv and load to mysql").getOrCreate()
    sc=spark.sparkContext
    return spark, sc

def read_csv(spark, sc):
    path=r'C:\Users\ravi\dataset'

    #files
    file1='links.csv'
    file2='movies.csv'
    file3='ratings.csv'
    file4='tags.csv'

    #dataframes

    #movieId|imdbId|tmdbId
    link_df=spark.read.csv(path +"\\"+file1, header="True", inferSchema="True")
    
    #movieId|title|genres
    movies_df=spark.read.csv(path +"\\"+file2, header="True", inferSchema="True")

    #userId|movieId|rating|timestamp
    ratings_df=spark.read.csv(path +"\\"+file3, header="True", inferSchema="True")

    #userId|movieId|tag|timestamp
    tags_df=spark.read.csv(path +"\\"+file4, header="True", inferSchema="True")

    return link_df, movies_df, ratings_df, tags_df

#Question: movie name, year, timestamp and genres having rating more than and equal to 5.

def calculation(spark, sc):
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    link_df, movies_df, ratings_df, tags_df=read_csv(spark, sc)
    link_df.registerTempTable("link")
    movies_df.registerTempTable("movies")
    ratings_df.registerTempTable("ratings")
    tags_df.registerTempTable("tags")
    
    output=spark.sql(""" SELECT movies.movieId, movies.title, movies.genres, ratings.userId, ratings.rating, ratings.timestamp FROM movies inner join ratings on movies.movieId=ratings.movieId where rating>=5 """)
    final=output.withColumn("timestamp", F.from_unixtime(output['timestamp'],"MM-dd-yyyy HH:mm:ss"))
    return final

def write_mysql(spark, sc):

    final=calculation(spark,sc)
    #mysql connection details
    driver = "com.mysql.jdbc.Driver"
    url = "jdbc:mysql://127.0.0.1:3306/test"
    user = "root"
    pwd = "India@123"
    final.write.format("jdbc").option("driver", driver)\
    .option("url", url)\
    .option("dbtable", "movies_ratings")\
    .option("user", user)\
    .option("password", pwd)\
    .save()

if __name__ == '__main__':
    spark, sc=create_session()
    read_csv(spark, sc)
    calculation(spark,sc)
    write_mysql(spark, sc)
