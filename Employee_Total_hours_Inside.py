import datetime
from pyspark.sql.types import StructType, StructField, TimestampType, LongType, StringType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum , unix_timestamp , lag, when ,col


spark = SparkSession.builder \
        .appName("TotalInTime") \
        .getOrCreate()

_data = [
    (11114, datetime.datetime.strptime('2024-01-01 08:30:00.00', "%Y-%m-%d %H:%M:%S.%f"), "I"),
    (11114, datetime.datetime.strptime('2024-01-01 10:30:00.00', "%Y-%m-%d %H:%M:%S.%f"), "O"),
    (11114, datetime.datetime.strptime('2024-01-01 11:30:00.00', "%Y-%m-%d %H:%M:%S.%f"), "I"),
    (11114, datetime.datetime.strptime('2024-01-01 15:30:00.00', "%Y-%m-%d %H:%M:%S.%f"), "O"),
    (11115, datetime.datetime.strptime('2024-01-01 09:30:00.00', "%Y-%m-%d %H:%M:%S.%f"), "I"),
    (11115, datetime.datetime.strptime('2024-01-01 17:30:00.00', "%Y-%m-%d %H:%M:%S.%f"), "O")
]

df = spark.createDataFrame(_data, ["emp_id", "punch_time", "flag"])

df.show(truncate=False)

windowspec = Window.partitionBy("emp_id").orderBy("punch_time")

df = df.withColumn("punch_in_time", lag(df.punch_time).over(windowspec))
df =        df.withColumn("time_diff", (unix_timestamp(df.punch_time) - unix_timestamp(df.punch_in_time))/60)


# df  = df.groupBy("emp_id").agg(sum("time_diff").alias("time_in")).select("emp_id","time_in")

df = df.groupBy('emp_id').agg(sum(when(col('flag') == 'O', col('time_diff')).otherwise(0)).alias('time_in'))

df.show(truncate=False)