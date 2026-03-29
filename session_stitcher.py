from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SeniorDEChallenge").getOrCreate()

# Sample Web Logs
logs_data = [
    ("2023-10-01 10:00:00", "dev_1", None, "click"),   # Guest click (Stitch this!)
    ("2023-10-01 11:00:00", "dev_1", 101, "click"),    # Logged-in click
    ("2023-09-20 10:00:00", "dev_1", None, "click"),   # Guest click (Too old - don't stitch)
    ("2023-10-01 10:30:00", "dev_2", None, "click"),   # Guest click (Stitch this!)
    ("2023-10-01 12:00:00", "dev_2", 102, "purchase")
]

# Sample Mapping
mapping_data = [
    ("dev_1", 101, "2023-10-01 11:00:00"),
    ("dev_2", 102, "2023-10-01 12:00:00")
]

logs_df = spark.createDataFrame(logs_data, ["event_time", "device_id", "user_id", "action"]) \
               .withColumn("event_time", F.to_timestamp("event_time"))

mapping_df = spark.createDataFrame(mapping_data, ["device_id", "user_id", "first_login_time"]) \
                  .withColumn("first_login_time", F.to_timestamp("first_login_time"))

logs_df.show()
mapping_df.show()

joined_df = logs_df.alias('logs').join(mapping_df.alias('map'), logs_df.device_id == mapping_df.device_id, "left")\
                   .select('logs.*', F.col('map.user_id').alias('map_user_id'),'map.first_login_time' )

# joined_df = joined_df.withColumn('user_id', F.when(F.col('user_id').isNotNull(), F.col("user_id"))\
#                                                     .otherwise(F.col('map_user_id'))).drop('map_user_id')

joined_df = joined_df.withColumn('user_id', F.when(((F.unix_timestamp(F.col('first_login_time')) - F.unix_timestamp(F.col('event_time'))) < 86400) & F.col('user_id').isNotNull(), F.col("user_id"))\
                                                    .otherwise('Guest'))


joined_df.show()