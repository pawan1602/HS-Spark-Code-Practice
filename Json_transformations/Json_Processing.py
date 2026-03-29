from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, IntegerType
import json

spark = SparkSession.builder\
                     .appName("json_process")\
                     .master("local[2]")\
                      .getOrCreate()


dict1 = {
  "orderId": "ORD1001",
  "customer": {
    "name": "Aman",
    "city": "Delhi",
    "email": "aman@example.com"
  },
  "items": [
    {"product": "Laptop", "price": 55000, "qty": 1},
    {"product": "Mouse", "price": 700, "qty": 2}
  ],
  "payment": {
    "mode": "UPI",
    "status": "Success"
  },
  "orderDate": "2024-11-10"
}
json_data = json.dumps(dict1)



# df.withColumn("orderId",explode(df.order_json)).show()


order_schema  = StructType([
    StructField("orderId", StringType()),
    StructField("customer", StructType([
        StructField("name", StringType()),
        StructField("city", StringType()),
        StructField("email", StringType())
    ])),
    StructField("items", ArrayType(StructType([
        StructField("product", StringType()),
        StructField("price", IntegerType()),
        StructField("qty", IntegerType())
    ]))),
    StructField("payment", StructType([
        StructField("mode", StringType()),
        StructField("status", StringType())
    ])),
    StructField("orderDate", StringType())
])


df = spark.createDataFrame([dict1], schema=order_schema)
df.printSchema()
df.show()
#
# df.select("orderId").withColumn("custom_name",explode("customer")).show()

df_items = df.select(
    "orderId",
    "customer.*",
    explode("items").alias("item"),
    "orderDate"
)

df_items.show()