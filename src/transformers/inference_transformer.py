from typing import Iterator
import pandas as pd
from pyspark.sql import functions as F
from src.dataAccessObjects.DAO import pklDAO, CSVDAO


class InferenceTransformer(pklDAO, CSVDAO):

    def __init__(self, spark):
        self.spark = spark
        super().__init__(spark=self.spark)

    def transform_data(self, **kwargs):
        model = kwargs["model"]
        data_pd = pd.DataFrame(data=kwargs["df"],
                               columns=[f'feat_{str(x)}' for x in range(1, kwargs["df"].shape[1] + 1)])
        data_df = self.spark.createDataFrame(data_pd)

        @F.pandas_udf("double")
        def infer(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
            for features in iterator:
                pdf = pd.concat(features, axis=1)
                yield pd.Series(model.predict_proba(pdf)[:, 1])

        pred_df = data_df.withColumn("prediction", infer(*data_df.columns))
        return pred_df
