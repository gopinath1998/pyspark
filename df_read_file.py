from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Dataframe by reading file").master("local[*]").getOrCreate()

print("create dataframe from a CSV file :")

df=spark.read.format("csv").option("header",True).option("inferScema",True).load("C:\SparkCourse\data\emp.csv")
df.printSchema()

print("Reading with Schema instead of inferring :")
_schema = "employee_id int, department_id int, name string, age int, gender string, salary double, hire_date date"

df_schema = spark.read.format("csv").option("header",True).schema(_schema).load("C:\SparkCourse\data\emp.csv")
df_schema.printSchema()
df_schema.show()


print("Dealing with BAD records using default PERMISSIVE mode :")
_schema_df = "employee_id int, department_id int, name string, age int, gender string, salary double,_corrupt_record string"
df_p= spark.read.format("csv").option("header",True).option("columnNameofCorruptedRecord","bad_record").schema(_schema_df).load("C:\SparkCourse\data\emp_data.csv")
df_p.printSchema()
print("printing not corrupted records: ")
df_p.where("_corrupt_record is not null").show(df_p.count())


print("Dealing with BAD records using DROPMAL FORMED mode :")

_schema_dm= "employee_id int, department_id int, name string, age int, gender string, salary double,_corrupt_record string"
df_dm= spark.read.format("csv").option("header",True).option("mode","DROPMALFORMED").schema(_schema_dm).load("C:\SparkCourse\data\emp_data.csv")
df_dm.printSchema()
print("DROPMALFORMED drops the corrupted records :")
df_dm.show(df_p.count())


print("Dealing with BAD records using FAIL FAST mode :")
print("FAILFAST mode fails the job as soon as it find mismatch in schema ")
_schema_ff= "employee_id int, department_id int, name string, age int, gender string, salary double,_corrupt_record string"
_schema_ff= spark.read.format("csv").option("header",True).option("mode","FAILFAST").schema(_schema_ff).load("C:\SparkCourse\data\emp_data.csv")
_schema_ff.printSchema()


print("Multiple options in single option")

_options={
    "header":"true",
    "inferSchema":"true",
    "mode":"PERMISSIVE"
}

df=spark.read.format("csv").options(**_options).load("C:\SparkCourse\data\emp_data.csv")


