from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from pyspark.sql.window import Window

from pyspark.sql.functions import desc, rank

'''
Deduplicate by user_id

Keep the latest record using updated_at '''

spark = SparkSession.builder\
              .appName("master")\
              .master("local[*]")\
               .getOrCreate()
data = [
    (101, "Alice",    "2024-01-10 09:15:00"),
    (101, "Alice M",  "2024-03-05 14:20:00"),
    (102, "Bob",      "2024-02-01 10:00:00"),
    (103, "Charlie",  "2024-01-25 08:45:00"),
    (102, "Bob K",    "2024-02-20 16:30:00"),
    (104, "David",    "2024-03-01 12:00:00"),
    (103, "Charlie",  "2024-02-10 11:10:00"),
    (101, "Alice S",  "2024-02-15 18:00:00")
]

schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("updated_at", StringType(), True)
])

df = spark.createDataFrame(data, schema)

df = df.withColumn("updated_at", df["updated_at"].cast("timestamp"))

df.show(truncate=False)
df.printSchema()


windowspac = Window.partitionBy("user_id").orderBy(desc("updated_at"))


df = df.withColumn("rnk", rank().over(windowspac))

df.filter(df["rnk"] == 1).select("user_id","name","updated_at").show()