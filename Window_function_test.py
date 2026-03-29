from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f


spark = SparkSession.builder\
                 .appName("window")\
                  .master("local[2]")\
                   .getOrCreate()


data = [
    ("UK",      47, 120, 5400, 10500.50),
    ("UK",      48,  98, 4800,  9200.00),
    ("UK",      49, 135, 6200, 11850.75),
    ("Germany", 47,  80, 3100,  7450.25),
    ("Germany", 48,  95, 3700,  8700.50),
    ("Germany", 49,  78, 2900,  6900.00),
]

columns = ["Country", "WeekNumber", "NumInvoices", "TotalQuantity", "InvoiceValue"]

summary_df = spark.createDataFrame(data, columns)

summary_df.show()

window_def = Window.partitionBy("Country").orderBy("WeekNumber")\
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)


new_df = summary_df.withColumn("InvoiceValue_running_total", f.avg("InvoiceValue").over(window_def))

new_df.show()