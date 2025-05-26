import unittest
from pyspark.sql import SparkSession
from scripts.etl import transform_data, filter_relevant_kickstarters

class TestETLPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("ETLTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform_data_adds_columns(self):
        df = self.spark.createDataFrame([
            {"goal": "1000", "pledged": "1500", "launched": "2016-01-01 12:00:00", "deadline": "2016-01-10 00:00:00", "backers": "15", "usd pledged": "1400"}
        ])
        result = transform_data(df)
        self.assertIn("launched_date", result.columns)
        self.assertIn("deadline_date", result.columns)
        self.assertEqual(result.schema["goal"].dataType.typeName(), "double")
        self.assertEqual(result.schema["backers"].dataType.typeName(), "integer")

    def test_filter_removes_invalid_rows(self):
        df = self.spark.createDataFrame([
            {"goal": 0.0, "launched_date": None, "deadline_date": None, "campaign_duration": -1},
            {"goal": 1000.0, "launched_date": "2020-01-01", "deadline_date": "2020-01-05", "campaign_duration": 4}
        ])
        filtered = filter_relevant_kickstarters(df)
        self.assertEqual(filtered.count(), 1)

if __name__ == "__main__":
    unittest.main()
