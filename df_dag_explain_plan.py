from pyspark.sql import SparkSession

import time
spark = (
    SparkSession
    .builder
    .appName("DAG and explain plan")
    .getOrCreate()
)

time.sleep(5)

print("Creating 2 dataframes using range :")
df_1=spark.range(2,200,4)
df_2=spark.range(4,200,2)

time.sleep(5)
print("Repartitioning the data:")
df_3=df_1.repartition(5)
df_4=df_2.repartition(7)

print("****** NOTE: By default shuffle writes data into 200 partitions**********")
df_joined=df_3.join(df_4,on="id")

df_sum=df_joined.selectExpr("sum(id) as total_sum")

df_sum.show()
df_sum.explain()


print("Understand the skipped stages :")
df_union=df_sum.union(df_4)
df_union.show()

df_union.explain()



time.sleep(120) # To keep the spark session on for N seconds so that we can see it in spark UI












