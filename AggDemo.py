from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as f

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config("AGG_APP_CONFIGS")
    spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()
    logger = Log4J(spark)

    invoices_df = get_raw_df_from_csv(spark, "data/invoices.csv")

    #Column object base expression
    invoices_df.select(f.count("*").alias("Count *"),
        f.sum("Quantity").alias("TotalQuantity"),
        f.avg("UnitPrice").alias("AvgPrice"),
        f.countDistinct("InvoiceNo").alias("CountDistinct")).show()

    #SQL expression
    invoices_df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as `TotalQuantity`",
        "avg(UnitPrice) as `AvgPrice`"
    ).show()

    #Group by using sql
    invoices_df.createOrReplaceTempView("invoice_tbl")
    invoice_sql = spark.sql("""
        select 
            Country, 
            InvoiceNo,
            sum(Quantity) as `TotalQuantity`,
            round(sum(Quantity*UnitPrice),2) as InvoiceValue
        from invoice_tbl
        group by Country, InvoiceNo
    """)
    invoice_sql.show()


    #Group by using dataframes
    summary_df = invoices_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity*UnitPrice")),2).alias("InvoiceValue"))
    summary_df.show()

    #Exercice
    exercice_df = invoices_df\
        .withColumn("InvoiceDate", f.to_date(f.split("InvoiceDate", ' ')[0], 'd-M-y'))\
        .withColumn("WeekNumber", f.weekofyear("InvoiceDate"))

    NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.round(f.sum(f.expr("Quantity*UnitPrice")),2).alias("InvoiceValue")
    exercice_df = exercice_df \
        .where("""WeekNumber is not null 
                and Country IN ('EIRE', 'France')
                and WeekNumber BETWEEN 48 AND 51
            """)\
        .groupBy("Country", "WeekNumber") \
        .agg(NumInvoices,
             TotalQuantity,
             InvoiceValue)

    exercice_df.orderBy("Country", "WeekNumber").show()
