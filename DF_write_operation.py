# from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.conf  import SparkConf



conf = SparkConf()\
       .setMaster("local[2]")\
        .setAppName("write_test")



# spark = SparkSession.builder\
#                     .appName("writer_test")\
#                      .master("local[2]")\
#                      .getOrCreate()

spark = SparkSession.builder\
                     .config(conf=conf) \
                     .getOrCreate()


salary_df = spark.read\
                 .format("csv")\
                 .option("header","true")\
                 .option("inferSchema","true")\
                 .load("csv_data/name_age_salary.csv")

salary_df.show()


salary_df.repartition(4).write\
      .format("csv")\
       .mode("errorifexists") \
       .save("csv_data/salary_df.csv")