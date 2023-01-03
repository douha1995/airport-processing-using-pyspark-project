# airport-processing-using-pyspark-project
#### This document is designed to be read in parallel with the code in the pyspark-template-project repository. Together, these constitute what we consider to be a 'best practices' approach to writing ETL jobs using Apache Spark and its Python ('PySpark') APIs. This project addresses the following topics:

    - how to structure ETL code in such a way that it can be easily tested and debugged;
    - how to pass configuration parameters to a PySpark job;
    - how to handle dependencies on other modules and packages; and,
    what constitutes a 'meaningful' test for an ETL job.

## Runing ETL Job
Assuming that the $SPARK_HOME environment variable points to your local Spark installation folder, then the ETL job can be run from the project's root directory using the following command from the terminal,
    $SPARK_HOME/bin/spark-submit \
    --master local[*] \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl.py
