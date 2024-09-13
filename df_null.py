from pyspark.sql import SparkSession
from pyspark.sql.functions import when,col,lit,coalesce,count

spark = (
    SparkSession
    .builder
    .appName("DF nulls")
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
    ["018","104","Nancy Liu","29","","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

# Create emp DataFrame
emp = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp.show()

print(" None will populate empty value as Null :")
emp_casted_null=emp.withColumn("new_gender",when( col("gender")=='Male','M' ).when(col("gender")=='Female','F').otherwise(None))
emp_casted_null.show()

print("dropping null record using na.drop :")
#emp_1=emp_casted_null.na.drop()
#emp_1.show()

#print("Replace null with other value :")
#emp_casted_null.fillna("0")

print("Replace null with other value :")
emp_null_replaced=emp_casted_null.withColumn("new_gender", coalesce("new_gender",lit("O")) )
emp_null_replaced.show()

print(" Drop unwanted columns and rename the column :")
emp_new_df=emp_null_replaced.drop("gender").withColumnRenamed("new_gender","gender")
emp_new_df.show()

print("Number of columns in a DF :")
for i in emp_casted_null.columns:
    print(i)

print("Iterating over all columns of a DF:\n")
for i in emp_casted_null.columns:
    if i != 'new_gender':
        print(f"number of null values in a column: '{i}' ")
        emp_new_df.select( count( when( col(i)=='Null',1) ).alias(i) ).show()

        print(f"number of not null values in a column: '{i}' ")
        emp_new_df.select( count( col(i)).alias(i)  ).show()





#This query count number of nulls in each columns
#data = [(1, 'Alice', 30), (2, 'Bob', None), (3, 'Null', 25), (4, 'Charlie', 'Null')]
#schema = ['id', 'name', 'age']

#df = spark.createDataFrame(data, schema)
#df_final = df.select([count(when(col(i) == 'Null', 1)).alias(i) for i in df.columns])

#df_final.show()
















