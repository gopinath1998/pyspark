from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,to_json,explode

spark = (
    SparkSession
    .builder
    .appName("Data Partition")
    .master("local[*]")
    .getOrCreate()
)

print("Read data from single line json file :")
df_single=spark.read.format("json").load("C:\SparkCourse\data\order_singleline.json")
df_single.printSchema()
df_single.show()

print("Read data from Multi line json file :")
df_multi=spark.read.format("json").option("multiline","true").load("C:\SparkCourse\data\order_multiline.json")
df_multi.printSchema()
df_multi.show()

print("Read json file as text file")
df=spark.read.format("text").load("C:\SparkCourse\data\order_singleline.json")
df.printSchema()
df.show(truncate=False)


print("Enforce the schema instead of inferring from single line json file :")
_schema_single="customer_id string,order_id string,contact array<long>"
df_schema_single=spark.read.format("json").schema(_schema_single).load("C:\SparkCourse\data\order_singleline.json")
df_schema_single.printSchema()
df_schema_single.show()


print("Enforce the schema instead of inferring from multi line json file :")
_schema_multi="contact array<long>,customer_id string,order_id string, order_line_items array< struct< amount double,item_id string,qty long > >"
df_schema_multi=spark.read.format("json").schema(_schema_multi).load("C:\SparkCourse\data\order_singleline.json")
df_schema_multi.printSchema()
df_schema_multi.show()

print("from_json function used to parse json from string type:")
df_expanded=df.withColumn("parsed",from_json(df.value,_schema_multi))
df_expanded.printSchema()


print("to_json used to convert json type to string type opposite of from_json")
df_parsed=df_expanded.withColumn("unparsed",to_json(df_expanded.parsed) )
df_parsed.printSchema()
df_parsed.show()
df_parsed.select("unparsed").show()

print(" Get values of a parsed JSON")
df_1=df_expanded.select("parsed.*")
df_1.printSchema()
df_1.show(truncate= False)

print("Explode used to populate every values in a array into new rows")
df_2=df_1.withColumn("expanded_line_items",explode("order_line_items"))
df_2.show()

print("select all the columns in a exploded column :")
df_3=df_2.select("contact","customer_id","order_id","expanded_line_items.*")
df_3.show()

print("select only specific column in a exploded column using exploded items attribute name using dot(.):")
df_2.select("contact","customer_id","order_id","expanded_line_items.item_id","expanded_line_items.qty").show()


print("Exploding contact array :")
df_final=df_3.withColumn("contact_expanded",explode("contact"))
df_final.drop("contact").show()





























