from time import perf_counter
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("SparkPerformanceLab")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Synthetic data avoids a large download and keeps the lab free.
orders = spark.range(0, 1_000_000).withColumnRenamed("id", "order_id")
orders = orders.withColumn("customer_id", (F.col("order_id") % 100_000).cast("long"))
customers = spark.range(0, 100_000).withColumnRenamed("id", "customer_id")
customers = customers.withColumn("segment", F.when(F.col("customer_id") % 2 == 0, "A").otherwise("B"))

def measure(label, df):
    start = perf_counter()
    result = df.count()
    elapsed = perf_counter() - start
    print(f"{label}: rows={result}, seconds={elapsed:.3f}")
    return elapsed

print("Regular join plan:")
regular = orders.join(customers, "customer_id", "left")
regular.explain("formatted")
measure("regular_join", regular)

print("Broadcast join plan:")
broadcast = orders.join(F.broadcast(customers), "customer_id", "left")
broadcast.explain("formatted")
measure("broadcast_join", broadcast)

# Example skew diagnostic.
orders.groupBy("customer_id").count().orderBy(F.desc("count")).show(10, truncate=False)

spark.stop()
