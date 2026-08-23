from pyspark.sql import SparkSession
import time

spark = (
    SparkSession.builder
    .appName("Midterm-Spark-UI-Demo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.range(0, 5000000, 1, 16)

print("Spark UI:", spark.sparkContext.uiWebUrl)
print("Partitions:", df.rdd.getNumPartitions())
print("Count:", df.count())
print("Spark UI will stay open for 3 minutes...")

input("Press ENTER after taking the Spark UI screenshot...")

spark.stop()

