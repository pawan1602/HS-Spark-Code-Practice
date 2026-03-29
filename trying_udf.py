
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from pyspark.sql.functions import udf

spark =  SparkSession.builder\
                     .appName("udf_test")\
                      .master("local[*]")\
                      .getOrCreate()



def to_upper(s):
    return s.upper()
def to_lower(s):
    return s.lower()

udf_def= udf(to_upper, StringType())

data = [(1, "Alice", 25), (2, "Bob", 30)]
df = spark.createDataFrame(data, ["ID", "Info","age"])
df.show(truncate=False)


df.withColumn("Info",udf_def("Info")).show()

