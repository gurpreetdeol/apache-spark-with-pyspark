import sys
from pyspark.sql import *


def load_survey_df(spark, param):
    df = spark.read.csv(param, header=True, inferSchema=True)
    return df


def count_by_country(partition_survey_df):
    return partition_survey_df.groupBy('country').count()


if __name__ == "__main__":
    # conf = get_spark_app_config()

    spark = SparkSession.builder.appName("SparkApp").master("local[2]").getOrCreate()

    # if len(sys.argv) != 2:
    #     print("Usage: SparkApp <filename>")
    #     sys.exit(-1)

    print("Starting SparkApp")
    survey_raw_df = load_survey_df(spark, "data/datacamp_ecommerce.csv")
    partition_survey_df = survey_raw_df.repartition(2)
    count_df = count_by_country(partition_survey_df)
    count_df.show()

    print("Finished SparkApp")
    spark.stop()