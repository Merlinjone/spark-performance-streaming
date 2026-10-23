# Project 2 — Spark Performance and Streaming Lab

## Goal
Demonstrate that you can measure and explain Spark performance choices rather than just use transformations.

## Part A: performance lab
1. Create a small synthetic dataset (code in `performance_lab.py`).
2. Compare a regular join with a broadcast join when one side is small.
3. Inspect `explain("formatted")` output.
4. Record wall-clock durations across several runs. Do not claim one approach is always faster; results depend on data size and environment.
5. Optional: experiment with repartitioning and skewed keys.

## Part B: streaming-style ingestion
1. Run `streaming_lab.py` locally with PySpark installed.
2. It watches an input folder for new JSON files and writes micro-batches to a Delta table.
3. Add new JSON files while the query runs.
4. Confirm checkpoint files are created and that repeated batches are handled safely.

Local requirements: Python 3.10 or 3.11, Java compatible with your installed Spark, `pyspark`, `delta-spark`. If local Java/Spark setup is troublesome, do the performance notebook in Databricks and document the streaming code as a locally runnable lab.
