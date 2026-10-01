"""Kafka -> Spark Structured Streaming -> windowed aggregates + dead-letter sink."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from streaming.transforms import validity_expr

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("device_id", StringType()),
    StructField("site", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("pressure_kpa", DoubleType()),
    StructField("event_ts", StringType()),
])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="sensor-events")
    p.add_argument("--out", default="output")
    a = p.parse_args()

    spark = SparkSession.builder.appName("sensor-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", a.bootstrap)
        .option("subscribe", a.topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )
    parsed = (
        raw.select(F.col("value").cast("string").alias("json"))
        .withColumn("e", F.from_json("json", SCHEMA))
        .select("json", "e.*")
        .withColumn("event_time", F.to_timestamp("event_ts"))
        .withColumn("is_valid", F.expr(validity_expr()))
    )

    (parsed.filter(~F.col("is_valid")).select("json")
        .writeStream.format("json")
        .option("path", f"{a.out}/dead_letter")
        .option("checkpointLocation", f"{a.out}/_chk/dead_letter")
        .start())

    agg = (
        parsed.filter("is_valid")
        .withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "5 minutes"), "site")
        .agg(
            F.count("*").alias("events"),
            F.approx_count_distinct("device_id").alias("devices"),
            F.avg("temperature_c").alias("avg_temp_c"),
            F.max("temperature_c").alias("max_temp_c"),
        )
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "*")
        .drop("window")
    )
    (agg.writeStream.outputMode("append").format("parquet")
        .option("path", f"{a.out}/site_metrics")
        .option("checkpointLocation", f"{a.out}/_chk/site_metrics")
        .trigger(processingTime="30 seconds")
        .start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
