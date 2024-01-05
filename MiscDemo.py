from pyspark.sql import *
from pyspark.sql.types import *

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config("MISC_APP_CONFIGS")
    spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()
    logger = Log4J(spark)

    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

    raw_df = spark.createDataFrame(data_list) \
        .toDF("name", "day", "month", "year") \
        .repartition(3)

    #Add a unique id column
    df1 = raw_df.withColumn("id", monotonically_increasing_id())
    df1.show()

    #Using case when to correct the year
    df2 = df1.withColumn("year", expr("""
        case when year < 21 then year + 2000
        when year < 100 then year + 1900
        else year
        end"""))

    #Cast
    #Recommended way
    df3 = df1.withColumn("year", expr("""
            case when year < 21 then cast(year as int) + 2000
            when year < 100 then cast(year as int) + 1900
            else year
            end"""))
    df3.show()
    #The schema didn't change
    df3.printSchema()

    #Other way
    df4 = df1.withColumn("year", expr("""
            case when year < 21 then year + 2000
            when year < 100 then year + 1900
            else year
            end""").cast(IntegerType()))
    #The schema changed
    df4.printSchema()

    #Fix the schema at the beginning
    df5 = raw_df.withColumn("id", monotonically_increasing_id()) \
        .withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType()))

    #use column object expression for the case when
    df6 = df5.withColumn("year", when(col("year") < 20, col("year") + 2000)
                    .when(col("year") < 100, col("year") + 1900)
                    .otherwise(col("year")))

    #Create a date of birth column
    df7 = df6.withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')"))

    #Drop useless columns
    df8 = df7.drop("day", "month", "year")

    #Drop duplicate lines and sort by date of birth
    final_df = df8.dropDuplicates(["name", "dob"]).sort(expr("dob desc"))

    final_df.show()
