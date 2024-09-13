from pyspark.sql import SparkSession
import pyspark
import time

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("Cache and persist")
    .config("spark.executor.memory", "512M")
    .getOrCreate()
)

_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"

df_schema = spark.read.format("csv").option("header",True).schema(_schema).load("C:\SparkCourse\data\emp.csv")

df_schema.filter("age>30").show()

print("Cache dataframe data  :")
df_schema.filter("age>30").cache()

print("Triggering the action to have cache :")
df_schema.filter("age>30").count()

print("Filtering on top of the cached result :")
print("In this case no cache will be used as the data is different :")
df_schema.filter("age>25").show()

print("******NOTE: To take advantage of Cache we need to use the same filter or same condition,else cache will not be used: ")

print("To Remove all the Caches")
spark.catalog.clearCache()

print("Memory levels for Cache:")
print("By default Cache used MEMORY_AND_DISK for DF and MEMORY_ONLY for RDD :")

print("We can change the memory levels using persist()")
# MEMORY_ONLY, MEMORY_AND_DISK, MEMORY_ONLY_SER, MEMORY_AND_DISK_SER, DISK_ONLY, MEMORY_ONLY_2, MEMORY_AND_DISK_2

# In pyspark when we persist using MEMORY_ONLY the data by default serialized
# In Pyspark we cannot use MEMORY_ONLY_SER
#df_persist=df_schema.persist(pyspark.StorageLevel.MEMORY_ONLY)

print("MEMORY_ONLY_2 replicates the data two times :")
df_persist=df_schema.persist(pyspark.StorageLevel.MEMORY_ONLY_2)

# format("noop") does write dataframe but it simulates an action
df_persist.write.format("noop").mode("overwrite").save()


print("unpersist the persisted data using below:")
df_persist.unpersist()


print("To Remove all the Caches")
spark.catalog.clearCache()

time.sleep(300)
