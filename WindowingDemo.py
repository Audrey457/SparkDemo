from pyspark.sql import *
from pyspark.sql import functions as f

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config("WINDOWING_APP_CONFIGS")
    spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()
    logger = Log4J(spark)

    summary_df = spark.read.parquet("output/*.parquet")
    summary_df.show()

    running_total_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary_df.withColumn("RunningTotal",
                          f.sum("InvoiceValue").over(running_total_window)) \
        .show()