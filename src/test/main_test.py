import unittest
from pyspark.sql import SparkSession


class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUp(cls):
        cls.spark = SparkSession.builder \
            .master("local") \
            .appName("Search Data Processing") \
            .getOrCreate()
