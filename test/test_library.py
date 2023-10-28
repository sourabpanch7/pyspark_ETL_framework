import unittest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import functions as F
from src.transformers.library_transformer import LibraryTransformer


class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Sample PySpark ETL").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class TestLibraryTransformation(PySparkTestCase):
    def test_single_space(self):
        sample_data = [
            {
                "series": "Harry Potter",
                "author": "J.K.Rowling",
                "books": [
                    {
                        "name": "Philosopher's Stone",
                        "year_of_release": 1996
                    }, {
                        "name": "Chamber of Secrets",
                        "year_of_release": 1998
                    }]},
            {
                "series": "BB",
                "author": "ABC",
                "books": [
                    {
                        "name": "PQR",
                        "year_of_release": 1996
                    }
                ]}
        ]

        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)

        library_obj = LibraryTransformer(spark=self.spark)
        transformed_df = library_obj.transform_data(df=original_df)

        expected_data = [{"series": "Harry Potter", "no_of_books": 2},
                         {"series": "BB", "no_of_books": 1}]

        expected_df = self.spark.createDataFrame(expected_data)
        expected_df = expected_df.withColumn("no_of_books", F.col("no_of_books").cast("Integer")).select("series",
                                                                                                         "no_of_books")

        assertDataFrameEqual(transformed_df, expected_df)


if __name__ == '__main__':
    unittest.main()
