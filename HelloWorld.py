from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("helloworldIntelliJ").master("local[*]").getOrCreate()

df=spark.read.format("csv").load("c:/SparkCourse/1800.csv").show(10,False)
















