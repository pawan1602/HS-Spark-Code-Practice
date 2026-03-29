from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import time

spark = (
    SparkSession.builder
    .appName("Real-Skew-Demo")
    .master("local[4]")  # limit cores
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "false")  # IMPORTANT
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# -------------------------
# Create skewed data
# -------------------------
data = []

for i in range(800_000):
    data.append(("skewed_key", i))

for i in range(200_000):
    data.append((f"key_{i % 50}", i))

df = spark.createDataFrame(data, ["key", "value"])

# -------------------------
# Expensive UDF (forces CPU skew)
# -------------------------
def expensive_op(x):
    total = 0
    for _ in range(2000):
        total += x % 7
    return total

slow_udf = udf(expensive_op, IntegerType())

# -------------------------
# Shuffle + CPU-heavy work
# -------------------------
result = (
    df
    .groupBy("key")
    .agg({"value": "sum"})
    .withColumn("heavy", slow_udf("sum(value)"))
)

result.show()

# Keep UI alive
print("Keeping Spark UI alive...")
time.sleep(600)
