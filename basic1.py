'''
DataFrame creation scripts
@author: Mr. Ravi Kumar

'''
def get_session():
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.appName('basic1').getOrCreate()
    sc=spark.sparkContext
    return sc, spark

def data_frame(sc,spark):
    headers = ("id", "name", "age")
    data = [(1, "Ravi" , 26),
    (2, "Bhoomika" , 23),
    (3, "James" , 21),
     (4, "Joe" , 19),
     (5, "Conor Ryan" , 20),
     (6, "Darragh" , 26),
     (7, "Alan", 31),
     (8, "Amit" , 32),
     (9, "Smitha" , 28),
     (10, "Alex" , 30),
     (11, "Denis", 29),
     (12, "Michal" , 34),
     (13, "John Mathew", 27),
     (14, "Jim Parker", 29),
     (15, "Sophia Ran", 25),
     (16, "Wendi Blake", 29),
     (17, "Stephan Lai", 32),
     (18, "Fay Van Damme", 22),
     (19, "Brevin Dice", 24),
     (20, "Regina Oleveria", 37),
     (21, "Rajat", 21),
     (22, "Sheetal", 32),
     (23, "James" , 21),
     (23, "James" , 21)]
    print("creating the datafram ...................")
    df=spark.createDataFrame(data, headers)
    return df
def get_data(df):
    df=data_frame(sc, spark)
    print("***********20 sample records ***********")
    df.show(truncate=0)
if __name__ == '__main__':
    sc, spark=get_session()
    data_frame(sc, spark)
    get_data(spark)