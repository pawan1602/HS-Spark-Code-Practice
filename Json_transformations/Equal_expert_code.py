from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, TimestampType, StructField, StringType
from pyspark.sql.functions import col, to_timestamp, year, weekofyear, avg, abs
from pyspark.sql.window import Window

spark = SparkSession.builder\
                    .appName("ee")\
                     .master("local[*]")\
                      .getOrCreate()


schema1 = StructType([StructField("CreationDate",TimestampType()),StructField("Id", StringType()), StructField("PostId", StringType()),StructField("VoteTypeId",StringType())])

voter_df = spark.read\
                 .format("json")\
                  .schema(schema1)\
                  .load(r"C:\Users\Equipp\Downloads\equal-experts-perceptive-engaging-sagacious-equipment-9c829268f4b5\uncommitted\votes.jsonl")


voter_df.printSchema()
voter_df.show()

voter_df = voter_df.select(
    col("Id").cast("int").alias("id"),
    col("PostId").cast("int").alias("post_id"),
    col("VoteTypeId").cast("int").alias("vote_type_id"),
    to_timestamp("CreationDate").alias("creation_date")
)

voter_df.printSchema()


# Create a view named outlier_weeks in the blog_analysis schema.
# It will contain the output of a SQL calculation for which weeks are regarded as outliers based on the vote data that was ingested.
# The view should contain the year, week number and the number of votes for the week for only those weeks which are determined to be outliers,
# according to the following rule:


voter_df = voter_df.withColumn("year", year("creation_date")).withColumn("weekofyear", weekofyear("creation_date"))
voter_df.show(10)

voter_df = voter_df.select("year","weekofyear").groupBy("year", "weekofyear").count().alias("weekly_count")




windowSpec = Window.orderBy("year") \
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = voter_df.withColumn(
    "avg_count",
    avg("count").over(windowSpec)
)

result.show()

# WHERE ABS(1 - (w.vote_count / m.avg_votes)) > 0.2

result.filter(abs(1 - ("count" / "avg_count")) > 0.2).show()