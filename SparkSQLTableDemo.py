from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import *

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config("SPARKSQLTABLE_APP_CONFIGS")
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)
    flightTimeParquetDF = get_raw_df_from_parquet(spark, "data/flight*.parquet")

    #Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    # Set the current database
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    #Write to a managed database in the current Apache Spark database
    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "ORIGIN", "OP_CARRIER") \
        .sortBy("ORIGIN", "OP_CARRIER") \
        .saveAsTable("flight_data_tbl")

    #Same as :
    # flightTimeParquetDF.write \
    #     .mode("overwrite") \
    #     .saveAsTable("AIRLINE_DB.flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))

