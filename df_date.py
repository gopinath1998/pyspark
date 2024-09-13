from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,current_date,current_timestamp,date_format

spark = (
    SparkSession
    .builder
    .appName("df dates")
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

emp.printSchema()

# Convert string to date Type
emp_date_convert=emp.withColumn("Hire_date_converted", to_date("hire_date",'yyyy-mm-dd'))
emp_date_convert.show()
emp_date_convert.printSchema()

# today's date and current timestamp
emp_dated=emp_date_convert.withColumn("today_date",current_date()).withColumn("timestamp_now",current_timestamp())
emp_dated.show(truncate=False)
emp_dated.printSchema()

# convert date into string
emp_date_part=emp_dated.withColumn("date_year",date_format("Hire_date_converted","dd/mm/yyyy"))
emp_date_part.show()

# fetching year, month, day from date using date_format
emp_date_parts=emp_dated.withColumn("year",date_format("Hire_date_converted","yyyy")).withColumn("month",date_format("Hire_date_converted","mm")).withColumn("day",date_format("Hire_date_converted","dd"))
emp_date_parts.show()

# Fetch timezone using z
emp_date_zone=emp_dated.withColumn("Zone",date_format("Hire_date_converted","z"))
emp_date_zone.show()





