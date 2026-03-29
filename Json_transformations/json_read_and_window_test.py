# from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, sum as _sum, desc, rank, row_number
from pyspark.sql.window import Window

spark = SparkSession\
            .builder\
            .appName("json_demo")\
            .master("local[*]")\
            .getOrCreate()


df = spark.read\
          .format("json")\
          .option("multiline",True)\
          .load("cust_data.json")
df = df.withColumn("items", explode(df.items))
df = df.select("customer_id","order_date","order_id",df.items.product_id.alias("product_id"), df.items.category.alias("category_id"), df.items.quantity.alias("quantity"), df.items.price.alias("price") )
df = df.withColumn("spending", df.quantity*df.price)
df.show()


df.groupBy("customer_id","order_date").agg(_sum("spending").alias("daily_spending")).show()

# Find highest spending order for each customer (Window Function)

windowspec = Window.partitionBy("customer_id").orderBy(desc("spending"))

df = df.withColumn("spend_rank", row_number().over(windowspec))

df.filter("spend_rank == 1").show()