# PYSPARK ETL FRAMEWORK

This is a sample PySpark based ETL framework which can be used for quick starting a Data Engineering project.

## Initial assumptions

1. This code is to be run on an existing spark distribution.
2. Please ensure that the python version in the environment where the .egg file is created and where it's to be executed, 
match.


## Steps to Run the code

1. Ensure that you are in the pyspark_ETL_framework directory.
2. Create the egg file using the below command.

**<"PATH TO DESIRED PYTHON INSTALLATION"> setup.py bdist_egg**


## Steps to Launch the App

1. Run the app via the **_spark_submit.sh_** script by providing the necessary command line arguments.

For e.g..

"sh spark_submit.sh LibraryDriver resources/configs/library_config.json"

Here
$1 => Job Name (Should match the driver class name)
$2 => Config File Path


## Steps to add new Jobs

0. Ensure that the DAO object for reading and writing from your desired source is present in _**src.dataAccessObjects**_
1. Create corresponding transformer class under **_src.transformers_**
2. Create corresponding driver class under **_src.drivers_**