from pyspark.sql import SparkSession
from pyspark.sql.functions import col,round,desc
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType

spark=SparkSession.builder.appName("Mintemp").master("local[*]").getOrCreate()

schema=StructType([
    StructField("Station_id",StringType(),nullable=True),
    StructField("date",IntegerType(),nullable=True),
    StructField("measure_type",StringType(),nullable=True),
    StructField("temperature",FloatType(),nullable=True),
    ])

df=spark.read.schema(schema).csv("c://SparkCourse/1800.csv")

df.printSchema()

min_temps=df.filter(df.measure_type=="TMIN")

station_temps=min_temps.select("Station_id","temperature")

min_station_temps=station_temps.groupby("Station_id").min("temperature")

station_temp_df=min_station_temps.withColumn("temperatures",round(col("min(temperature)")*0.1*(9.0/5.0)+32.0,2)).select("Station_id","temperatures").sort(desc("temperatures"))

results=station_temp_df.collect()

for result in results:
    print(result[0]+"\t {:.2f}F".format(result[1]))

spark.stop()