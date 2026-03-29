from pyspark.sql import SparkSession

from pyspark.sql.functions import dense_rank, desc

from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

employee_data = [
    (1, "Joe",   70000, 1),
    (2, "Jim",   90000, 1),
    (3, "Henry", 80000, 2),
    (4, "Sam",   60000, 2),
    (5, "Max",   90000, 1)
]

employee_columns = ["id", "name", "salary", "departmentId"]

employee_df = spark.createDataFrame(employee_data, employee_columns)

employee_df.show()
employee_df.printSchema()


department_data = [
    (1, "IT"),
    (2, "Sales")
]

department_columns = ["id", "name"]

department_df = spark.createDataFrame(department_data, department_columns)

department_df.show()
department_df.printSchema()


windowspec = Window.partitionBy("departmentId").orderBy(desc("salary"))

employee_df = employee_df.withColumn("sal_rnk", dense_rank().over(windowspec))

employee_df.show()

employee_df.join(department_df, on=employee_df.departmentId == department_df.id).select(department_df.name,employee_df.name,employee_df.salary)\
            .filter(employee_df.sal_rnk == 1).show()
