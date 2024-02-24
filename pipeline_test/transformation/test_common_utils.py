from pyspark.sql import SparkSession
import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))
sys.path.insert(0,root_dir)

from pipeline.transformation.common_utils import standardize_column_names, transform_date

# Fixture to create a SparkSession for testing
import pytest

spark = SparkSession.builder.appName("test").getOrCreate()

class TestStandardizeColumnNames(object):

       
    def test_standardize_column_names(self):
        test_df  = spark.createDataFrame([(1, "a", "b", "c")], ["col1", "col 2", "col,3", "col;4"])
        expected_df = spark.createDataFrame([(1, "a", "b", "c")], ["col1", "col_2", "col3", "col4"])
        df = standardize_column_names(test_df)
        assert df.columns == expected_df.columns



class TestTransfromDate(object):

    def test_transform_date(self):
        test_df  = spark.createDataFrame([(1, "2020-01-01")], ["id", "date"])
        expected_df = spark.createDataFrame([(1, "2020-01-01", "2020-01-01")], ["id", "date", "date_transformed"])
        df = transform_date(test_df)
        assert df.select('date') == expected_df.select('date_transformed')


