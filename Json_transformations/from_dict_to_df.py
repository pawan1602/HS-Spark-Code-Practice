from codecs import StreamWriter

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, from_json
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("from_json_example").getOrCreate()

# Sample JSON data as a string column
data = [("John", '{"age": 30, "city": "New York"}'), ("Alice", '{"age": 25, "city": "London"}')]
df = spark.createDataFrame(data, ["name", "json_data"])
df.show(truncate=False)

schema = StructType([StructField("age", StringType(), True), StructField("city", StringType(), True)])

df_parsed = df.withColumn("parsed_data", from_json(df.json_data , schema ))
df_parsed.select("name","parsed_data.*").show()