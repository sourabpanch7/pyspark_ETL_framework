spark-submit \
--verbose \
--master local[3] \
--conf "spark.pyspark.python=/Users/sourabpanchanan/anaconda3/envs/pyspark_ETL_framework/bin/python" \
--conf "spark.pyspark.driver.python=/Users/sourabpanchanan/anaconda3/envs/pyspark_ETL_framework/bin/python" \
--conf "spark.pyspark.executor.python=/Users/sourabpanchanan/anaconda3/envs/pyspark_ETL_framework/bin/python" \
--py-files dist/pyspark_ETL_framework-0.1.0-py3.9.egg \
main.py \
--config_file $2 \
--job_name $1