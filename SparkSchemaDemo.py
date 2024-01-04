from pyspark.sql import *
from pyspark.sql.types import *

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config("SPARKSCHEMA_APP_CONFIGS")
    spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()

    logger = Log4J(spark)

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType()),
    ])

    flightSchemaDDL = ("FL_DATE DATE,OP_CARRIER STRING,OP_CARRIER_FL_NUM INT, "
                       "ORIGIN STRING,ORIGIN_CITY_NAME STRING,"
                       "DEST STRING,DEST_CITY_NAME STRING,"
                       "CRS_DEP_TIME INT,DEP_TIME INT,WHEELS_ON INT,"
                       "TAXI_IN INT,CRS_ARR_TIME INT,ARR_TIME INT,"
                       "CANCELLED INT,DISTANCE INT")

    flightTimeCsvDF = get_raw_df_from_csv(spark, "data/flight*.csv", flightSchemaStruct)
    flightTimeCsvDF.show(5)
    logger.info("CSV schema: " + flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF = get_raw_df_from_json(spark, "data/flight*.json", flightSchemaDDL)
    flightTimeJsonDF.show(5)
    logger.info("Json schema: " + flightTimeJsonDF.schema.simpleString())

    flightTimeParquetDF = get_raw_df_from_parquet(spark, "data/flight*.parquet")
    flightTimeParquetDF.show(5)
    logger.info("parquet schema: " + flightTimeParquetDF.schema.simpleString())