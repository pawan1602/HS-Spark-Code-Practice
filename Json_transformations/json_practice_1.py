
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, sum as _sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, BooleanType

spark =  SparkSession.builder.appName("json_practice_1")\
                       .master("local[2]")\
                       .getOrCreate()

data = {
  "orderId": "ORD9081",
  "customer": {
    "id": 101,
    "name": "Suman",
    "location": { "city": "Pune", "pin": 411002 }
  },
  "items": [
    {"sku": "MOB001", "price": 14000, "qty": 1, "category": "Mobile"},
    {"sku": "COV035", "price": 350, "qty": 2, "category": "Accessories"}
  ],
  "discounts": {
    "coupon": "NOV10",
    "amount": 500
  },
  "payment": {
    "method": "Card",
    "success": True
  },
  "timestamp": "2024-11-20T09:40:00"
}


schema1 = StructType([StructField("orderId", StringType()),
                    StructField("customer", StructType([
                        StructField("id", IntegerType()),
                        StructField("name", StringType()),
                        StructField("location",StructType([
                            StructField("city",StringType()),
                            StructField("pin",IntegerType())
                        ]))
                    ])),

                      StructField("items", ArrayType(StructType([
                          StructField("sku", StringType()),
                          StructField("price",IntegerType()),
                          StructField("qty",IntegerType()),
                          StructField("category",StringType())
                      ]))),

                      StructField("discounts", StructType([
                          StructField("coupon", StringType()),
                          StructField("amount",IntegerType())

                      ])),

                      StructField("payment", StructType([
                          StructField("method", StringType()),
                          StructField("success", BooleanType())

                      ])),

                      StructField("timestamp", StringType())





                      ])

df = spark.createDataFrame([data], schema=schema1)

df.printSchema()
df.show()

from pyspark.sql.functions import explode, col

df_flat = (
    df
    .select(
        "orderId",
        "timestamp",

        # Flatten top-level customer fields
        "customer.id",
        "customer.name",

        # Flatten customer.location fields
        "customer.location.city",
        "customer.location.pin",

        # Explode items
        explode("items").alias("item"),

        # Flatten discounts
        "discounts.coupon",
        "discounts.amount",

        # Flatten payment
        "payment.method",
        "payment.success"
    )
    # item is a struct → flatten it
    .select(
        "orderId",
        "timestamp",
        "id",
        "name",
        "city",
        "pin",
        "coupon",
        "amount",
        "method",
        "success",
        col("item.sku").alias("sku"),
        col("item.price").alias("price"),
        col("item.qty").alias("qty"),
        col("item.category").alias("category")
    )
)

df_flat.show(truncate=False)


# Compute total order value per order.

df_total = df_flat.groupBy("orderId").agg(_sum(col("price")*col("qty")).alias("order_value"))

# df_total.show()
# Compute total amount spent per customer.
df_amt_spend = df_flat.groupBy("name").agg(_sum(col("price")*col("qty")).alias("order_amount"))
# df_amt_spend.show()
# Compute total quantity sold per category.

df_quantity_cat = df_flat.groupBy("category").agg(_sum(col("qty")).alias("quantity"))
df_quantity_cat.show(truncate=False)