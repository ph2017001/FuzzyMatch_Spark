from datetime import date
from unittest import TestCase

from pyspark.sql import *
from pyspark.sql.types import *

from lib.utils import get_pandas_df, create_pipeline


class FuzzyMatchTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("FuzzyMatch_Spark") \
            .getOrCreate()

    def test_datafile_loading(self):
        ref_df = get_pandas_df("data/sample_test.xlsx", "reference")
        que_df = get_pandas_df("data/sample_test.xlsx", "query")
        self.assertEqual(len(ref_df), 39, "Reference Record count should be 40 in sample")
        self.assertEqual(len(que_df), 3, "Query Record count should be 3 in sample")

    def test_pipeline_stages(self):
        test_pipeline = create_pipeline()
        self.assertEqual(len(test_pipeline.getStages()), 8, "8 Stages should be present in the pipeline")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
