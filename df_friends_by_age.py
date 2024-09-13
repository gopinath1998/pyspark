from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark=SparkSession.builder.appName("friends_by_age").getOrCreate()

lines=spark.read.option("header","true").option("inferSchema","true").csv("C:///SparkCourse/fakefriends-header.csv")

lines.show()

friendsByAge=lines.select("age","friends")

friendsByAge.groupby("age").avg("friends").show()

friendsByAge.groupby("age").agg( func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()
 