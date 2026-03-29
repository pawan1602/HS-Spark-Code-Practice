from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("CallSummary").getOrCreate()

# Create the DataFrame (similar to your Insert query)
data = [
    (10, 20, 58), (20, 10, 12), (10, 30, 20),
    (30, 40, 200), (30, 40, 300), (40, 30, 500)
]
columns = ["from_id", "to_id", "duration"]
df = spark.createDataFrame(data, columns)

# Perform the transformation
result_df = df.withColumn("Person1", F.least("from_id", "to_id")) \
              .withColumn("Person2", F.greatest("from_id", "to_id")) \
              .groupBy("Person1", "Person2") \
              .agg(
                  F.count("*").alias("call_count"),
                  F.sum("duration").alias("total_duration")
              )

result_df.show()