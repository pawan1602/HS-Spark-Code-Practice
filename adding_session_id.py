from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType

from pyspark.sql.window import Window

from pyspark.sql.functions import lead, when, lag, unix_timestamp, sum, concat, lit
from requests import session

spark = SparkSession.builder\
                     .appName("session_id")\
                      .master("local[*]")\
                     .getOrCreate()

data = [
    (1, "2024-01-01 09:00:00"),
    (1, "2024-01-01 09:10:00"),
    (1, "2024-01-01 09:25:00"),
    (1, "2024-01-01 10:10:00"),
    (1, "2024-01-01 10:20:00"),
    (2, "2024-01-01 08:00:00"),
    (2, "2024-01-01 08:15:00"),
    (2, "2024-01-01 09:00:00"),
    (3, "2024-01-01 12:00:00"),
    (3, "2024-01-01 12:45:00")
]

lead_window = Window.partitionBy("user_id").orderBy("event_time")

df = spark.createDataFrame(data, ["user_id", "event_time"])
df = df.withColumn("event_time", df.event_time.cast("timestamp"))
df = df.withColumn("prv_time", lag(df.event_time).over(lead_window))

df = df.withColumn("session", when(df.prv_time.isNull(),"1")\
                                       .when((unix_timestamp(df.event_time) - unix_timestamp(df.prv_time))/60 > 30,1)\
                                        .otherwise(0))

sum_window = Window.partitionBy("user_id").orderBy("event_time")
df = df.withColumn("session_number", concat(lit("S_"),sum(df.session).over(sum_window).cast(IntegerType()).cast(StringType())))
df.show(truncate=False)
