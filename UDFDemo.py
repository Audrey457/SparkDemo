from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config("UDF_APP_CONFIGS")
    spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()
    logger = Log4J(spark)

    survey_df = get_raw_df_from_csv(spark, "data/survey.csv")

    survey_df.show(10)

    #Register the function to the driver as a dataframe udf
    parse_gender_udf = udf(parse_gender, StringType())
    #Use it in a column object expression
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    #Query the catalog
    logger.info("Catalog entry: ")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    #Register the udf as a SQL expression
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    #Use it in a SQL expression
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)

    # Query the catalog
    logger.info("Catalog entry: ")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]