from unittest import TestCase
from datetime import date

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.utils import *


class RowDemoTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        conf = get_spark_app_config("ROW_APP_CONFIGS")
        cls.spark = SparkSession.builder \
            .config(conf = conf) \
            .getOrCreate()

        my_schema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())
        ])

        my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    def test_data_type(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)

    def test_date_value(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertEqual(row["EventDate"], date(2020,4,5))

