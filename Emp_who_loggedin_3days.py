from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, datediff, col

spark = SparkSession.builder.getOrCreate()

data = [
    (101, "2024-01-01"),
    (101, "2024-01-02"),
    (101, "2024-01-03"),  # 3 consecutive ✅

    (102, "2024-01-01"),
    (102, "2024-01-03"),
    (102, "2024-01-04"),  # not consecutive ❌

    (103, "2024-01-10"),
    (103, "2024-01-11"),
    (103, "2024-01-12"),  # 3 consecutive ✅
    (103, "2024-01-13"),

    (104, "2024-01-05"),
    (104, "2024-01-06"),  # only 2 days ❌

    (105, "2024-01-01"),
    (105, "2024-01-04"),
    (105, "2024-01-07")  # scattered ❌
]

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("login_date", StringType(), True)
])

df = spark.createDataFrame(data, schema)

df = df.withColumn("login_date", df.login_date.cast("date"))

df.show()

windowspec = Window.partitionBy("emp_id").orderBy("login_date")

df = df.withColumn("prev_day", lag("login_date").over(windowspec))\
       .withColumn("next_day", lead("login_date").over(windowspec))

df.show()

df.filter(    col("prev_day").isNotNull() &
    col("next_day").isNotNull() & (datediff(col("next_day"), col("login_date")) == 1)  & (datediff(col("login_date"), col("prev_day")) == 1))\
    .select("emp_id").distinct().show()