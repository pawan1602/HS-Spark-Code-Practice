
'''
Given a clickstream of user activity data , find the relevant user session for each click event.

session definition:
1. session expires after inactivity of 30mins, because of inactivity no clickstream will be generated
2. session remain active for total of 2 hours



'''




from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import to_timestamp, lead, col, unix_timestamp, lag, when
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("ClickstreamSessionData") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ("2018-01-01 11:00:00", "u1"),
    ("2018-01-01 12:00:00", "u1"),
    ("2018-01-01 13:00:00", "u1"),
    ("2018-01-01 13:00:00", "u1"),
    ("2018-01-01 14:00:00", "u1"),
    ("2018-01-01 15:00:00", "u1"),
    ("2018-01-01 11:00:00", "u2"),
    ("2018-01-02 11:00:00", "u2")
]

schema = StructType([
    StructField("click_time", StringType(), True),
    StructField("user_id", StringType(), True)
])

df = spark.createDataFrame(data, schema) \
          .withColumn("click_time", to_timestamp("click_time"))

df.show(truncate=False)
df.printSchema()

next_click_time = df.withColumn("prev_click_time", lag(df.click_time, 1).over(Window.partitionBy("user_id").orderBy("click_time")))

# next_click_time = next_click_time.withColumn("time_diff_in_min",(unix_timestamp( col("click_time")) - unix_timestamp(col("prev_click_time")))/60)

next_click_time = next_click_time.withColumn("time_diff_in_min",( col("click_time").cast("long") - col("prev_click_time").cast("long"))/60)
next_click_time.show()

next_click_time = next_click_time.withColumn("time_diff_in_min", when(col("time_diff_in_min").isNull(), 0).otherwise(col("time_diff_in_min")))

next_click_time.withColumn("Session_id",  when(col("time_diff_in_min")>30, 1)\
                                                                                .otherwise(0)).show()

