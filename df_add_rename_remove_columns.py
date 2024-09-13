from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

spark = (
    SparkSession
    .builder
    .appName("Add Rename Drop columns")
    .master("local[*]")
    .getOrCreate()
)

emp_data = [
    ["001","101","John Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jane Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brown","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Chan","40","Male","60000","2013-04-01"],
    ["006","103","Jill Wong","32","Female","52000","2018-07-01"],
    ["007","101","James Johnson","42","Male","70000","2012-03-15"],
    ["008","102","Kate Kim","29","Female","51000","2019-10-01"],
    ["009","103","Tom Tan","33","Male","58000","2016-06-01"],
    ["010","104","Lisa Lee","27","Female","47000","2018-08-01"],
    ["011","104","David Park","38","Male","65000","2015-11-01"],
    ["012","105","Susan Chen","31","Female","54000","2017-02-15"],
    ["013","106","Brian Kim","45","Male","75000","2011-07-01"],
    ["014","107","Emily Lee","26","Female","46000","2019-01-01"],
    ["015","106","Michael Lee","37","Male","63000","2014-09-30"],
    ["016","107","Kelly Zhang","30","Female","49000","2018-04-01"],
    ["017","105","George Wang","34","Male","57000","2016-03-15"],
    ["018","104","Nancy Liu","29","Female","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

# Create emp DataFrame
emp = spark.createDataFrame(data=emp_data, schema=emp_schema)

emp_casted = emp.select("employee_id", "name", "age", col("salary").cast("double"))
emp_casted.printSchema()

# Add new column to a dataframe
emp_taxed=emp_casted.withColumn("tax",col("salary")*0.2)
emp_taxed.show()

# lit() method id used to populate static values
emp_new_cols=emp_taxed.withColumn("columnOne",lit(1)).withColumn("columnTwo",lit("Two"))
emp_new_cols.show()

# Rename a column
emp_1=emp_new_cols.withColumnRenamed("employee_id","empId")
emp_1.show()

# Rename column with space( not a best practice to have space in column name)
emp_2=emp_new_cols.withColumnRenamed("columnTwo","column Two")
emp_2.show()

emp_dropped=emp_new_cols.drop("columnTwo")
emp_dropped.show()

# TO drop multiple columns we can use comma in drop
emp_dropped_multiple=emp_new_cols.drop("columnOne","columnTwo")
emp_dropped_multiple.show()

# Filter data
emp_filtered=emp_dropped.where("tax>10000")
emp_filtered.show()

# Limit data
emp_limit=emp_filtered.limit(7)
emp_limit.show()

# Add multiple columns to a dataframe

columns={
    "tax": col("salary")*.20,
    "oneNumber":lit(1),
    "twoLiteral":lit("Two")
}
emp_final=emp.withColumns(columns)
emp_final.show()




















