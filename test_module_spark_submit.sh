spark-submit \
--verbose \
--master local[3] \
--conf "spark.pyspark.python=<ENTER PYTHON PATH HERE>" \
--conf "spark.pyspark.driver.python=<ENTER PYTHON PATH HERE>" \
--conf "spark.pyspark.executor.python=<ENTER PYTHON PATH HERE>" \
--py-files dist/pyspark_ETL_framework-0.1.0-py3.9.egg \
test/test_library.py