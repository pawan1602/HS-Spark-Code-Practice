
from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.appName("json_practice_2").master("local[3]").getOrCreate()

data = {
  "userId": 201,
  "device": {
    "model": "Samsung S21",
    "os": "Android"
  },
  "events": [
    {"type": "login", "ts": 1700000100},
    {"type": "page_view", "page": "home", "ts": 1700000150},
    {"type": "purchase", "amount": 600, "ts": 1700001200}
  ],
  "session": {
    "start": 1700000000,
    "end": 1700002000
  }
}


schema1 = StructType([StructField("userId", StringType(), True),
                      StructField("device", StructType([
                          StructField("model", StringType(), True),
                          StructField("os", StringType(), True),

                      ]), True),
                      StructField("events", ArrayType(StructType([
                          StructField("type", StringType(), True),
                          StructField("page", StringType())

                      ]
                      )))



                      ])