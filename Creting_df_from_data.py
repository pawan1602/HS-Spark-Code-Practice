from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType
from pyspark.sql.types import StructType, StructField

spark = SparkSession.builder\
                    .appName("dfdf")\
                     .master("local[2]")\
                     .getOrCreate()



data = [("ravi",37,"doctor"),
        ("pawan",45,"teacher"),
        ("ram",40,"IT") ]

schema = StructType([StructField("name",StringType(),True),
                    StructField("age",IntegerType(),True),
                    StructField("gender",StringType(),True)])

data_df = spark.createDataFrame(data, schema)

# data_df.printSchema()
# data_df.show()

data2 = [["ravi",37,"doctor"]]

rdd = spark.sparkContext.parallelize(data2)

# print(rdd.collect())


# convert rdd to df
df = rdd.toDF()

df.show()

# convert df to rdd
print(df.rdd.getNumPartitions())