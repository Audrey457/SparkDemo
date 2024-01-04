from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config("SPARKSCHEMA_APP_CONFIGS")
    spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()

    logger = Log4J(spark)

    flightTimeCsvDF = get_raw_df_from_csv(spark, "data/flight*.csv")
    flightTimeCsvDF.show(5)
    logger.info("CSV schema: " + flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF = get_raw_df_from_json(spark, "data/flight*.json")
    flightTimeJsonDF.show(5)
    logger.info("Json schema: " + flightTimeJsonDF.schema.simpleString())

    flightTimeParquetDF = get_raw_df_from_parquet(spark, "data/flight*.parquet")
    flightTimeParquetDF.show(5)
    logger.info("parquet schema: " + flightTimeParquetDF.schema.simpleString())