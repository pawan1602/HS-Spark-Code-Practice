from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import  sum

# Initialize Spark session
spark = SparkSession.builder.appName("RollingSumExample").getOrCreate()
data = [("2025–04–01", 100), ("2025–04–02", 200), ("2025–04–03", 150),
("2025–04–04", 250), ("2025–04–05", 300), ("2025–04–06", 350), ("2025–04–07", 400)]

df = spark.createDataFrame(data, ["Date", "Sales"])
df.show()

windowspec = Window.orderBy("Date").rowsBetween(Window.unboundedPreceding,Window.currentRow)

df = df.withColumn("Sales", df["Sales"].cast(IntegerType()))

running_sum_df = df.withColumn("running_sum", sum(df.Sales).over(windowspec))

running_sum_df.show()