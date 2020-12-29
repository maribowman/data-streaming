import logging

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# {
#    "crime_id": "183653601",
#    "original_crime_type_name": "Fireworks",
#    "report_date": "2018-12-31T00:00:00.000",
#    "call_date": "2018-12-31T00:00:00.000",
#    "offense_date": "2018-12-31T00:00:00.000",
#    "call_time": "23:01",
#    "call_date_time": "2018-12-31T23:01:00.000",
#    "disposition": "ADV",
#    "address": "800 Block Of Moscow St",
#    "city": "San Francisco",
#    "state": "CA",
#    "agency_id": "1",
#    "address_type": "Common Location",
#    "common_location": "Crocker Amazon Plgd, Sf"
# }

schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])


def run_spark_job(spark):
    # Create Spark configurations with max offset of 200 per trigger
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sf.police.department.calls") \
        .option('startingOffsets', 'earliest') \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 20) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")

    # select original_crime_type_name and disposition
    distinct_table = service_table \
        .select("original_crime_type_name", "disposition", "call_date_time") \
        .distinct() \
        .withWatermark("call_date_time", "1 minute")

    # count the number of original crime type
    agg_df = distinct_table \
        .dropna(how="any") \
        .select("original_crime_type_name") \
        .groupBy("original_crime_type_name") \
        .count() \
        .sort("count", ascending=True)

    # write output stream
    query = agg_df \
        .writeStream \
        .trigger(processingTime="1 seconds") \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    # attach a ProgressReporter
    query.awaitTermination()

    # get the right radio code json path
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # rename disposition_code column to disposition
    radio_code_df = radio_code_df \
        .withColumnRenamed("disposition_code", "disposition")

    # join on disposition column
    join_query = agg_df \
        .join(radio_code_df, agg_df.disposition == radio_code_df.disposition, "left")
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.driver.cores", 2) \
        .config("spark.executor.cores", 2) \
        .config("spark.default.parallelism", 2) \
        .getOrCreate()
    logger.info("Spark started")
    run_spark_job(spark)
    spark.stop()
