"""
Structured Streaming starter.
Run from this folder after installing compatible pyspark and delta-spark packages.
Input JSON example: {"event_id":"e1","order_id":"o1","status":"created"}
"""
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip

BASE = Path("./streaming_demo").resolve()
INPUT = BASE / "input"
OUTPUT = BASE / "delta_output"
CHECKPOINT = BASE / "checkpoint"
INPUT.mkdir(parents=True, exist_ok=True)

builder = (
    SparkSession.builder.appName("RetailPulseStreamingLab")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

events = (
    spark.readStream
    .schema("event_id STRING, order_id STRING, status STRING")
    .json(str(INPUT))
    .withColumn("processed_at", F.current_timestamp())
)

query = (
    events.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", str(CHECKPOINT))
    .start(str(OUTPUT))
)

print(f"Streaming is running. Add JSON files under: {INPUT}")
print("Stop with Ctrl+C. Delta output folder:", OUTPUT)
query.awaitTermination()
