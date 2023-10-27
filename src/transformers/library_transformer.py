from pyspark.sql import functions as F
from src.dataAccessObjects.DAO import JSONDAO, CSVDAO


class LibraryTransformer(JSONDAO, CSVDAO):

    def __init__(self, spark):
        self.spark = spark
        super().__init__(spark=self.spark)

    def transform_data(self, **kwargs):
        return kwargs["df"].select("series", F.size("books").alias("no_of_books"))
