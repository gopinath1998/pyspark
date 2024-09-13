from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, count, lit
import time

spark = (
    SparkSession
    .builder
    .appName("Optimizing Skewness and Spillage")
    .master("local[*]")
    .config("spark.cores.max", 8)
    .config("spark.executor.cores", 4)
    .config("spark.executor.memory", "512M")
    .getOrCreate()
)


# Disable AQE and Broadcast join
spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# Read Employee data
_schema = "first_name string, last_name string, job_title string, dob string, email string, phone string, salary double, department_id int"
emp = spark.read.format("csv").schema(_schema).option("header", True).load("C:\SparkCourse\data\employee_records_skewed.csv")


# Read DEPT CSV data
_dept_schema = "department_id int, department_name string, description string, city string, state string, country string"
dept = spark.read.format("csv").schema(_dept_schema).option("header", True).load("C:\SparkCourse\data\department_data.csv")


# Join Datasets

df_joined = emp.join(dept, on=emp.department_id==dept.department_id, how="left_outer")
df_joined.write.format("noop").mode("overwrite").save()

#Explain Plan
df_joined.explain()


# Check the partition details to understand distribution
part_df = df_joined.withColumn("partition_num", spark_partition_id()).groupBy("partition_num").agg(count(lit(1)).alias("count"))

part_df.show()


emp.groupBy("department_id").agg(count(lit(1))).show()














