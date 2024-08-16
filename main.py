import sys
from pyspark.sql import *

if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession.builder.appName("SparkApp").master("local[2]").getOrCreate()

    if len(sys.argv) != 2:
        print("Usage: SparkApp <filename>")
        sys.exit(-1)

    print("Starting SparkApp")
    survey_raw_df = load_survey_df(spark, sys.argv[1])
    partition_survey_df = survey_raw_df.repartition(2)
    count_df = count_by_country(partition_survey_df)
    count_df.show()

    print("Finished SparkApp")
    spark.stop()
