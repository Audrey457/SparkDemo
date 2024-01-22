from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Shuffle Join Demo") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4J(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    #Ensure that we get three partition after the shuffle
    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    #Broadcast the smaller dataframe to all executors.
    join_df = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner")

    join_df.foreach(lambda f: None)
    # join_df.collect()
    input("press a key to stop...")