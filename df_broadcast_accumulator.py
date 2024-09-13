from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,sum
import time
spark = (
    SparkSession
    .builder
    .appName("Distributed Shared Variables")
    .config("spark.executor.cores", 4)
    .config("spark.executor.memory", "512M")
    .getOrCreate()
)

# Read EMP CSV data

_schema = "first_name string, last_name string, job_title string, dob string, email string, phone string, salary double, department_id int"

emp = spark.read.format("csv").schema(_schema).option("header", True).load("C:\SparkCourse\data\employee_records.csv")

# Variable (Lookup)
dept_names = {1 : 'Department 1',
              2 : 'Department 2',
              3 : 'Department 3',
              4 : 'Department 4',
              5 : 'Department 5',
              6 : 'Department 6',
              7 : 'Department 7',
              8 : 'Department 8',
              9 : 'Department 9',
              10 : 'Department 10'}


# Broadcast the variable
broadcast_dept_names = spark.sparkContext.broadcast(dept_names)
# Check the value of the variable
broadcast_dept_names.value

# Create UDF to return Department name
@udf
def get_dept_names(dept_id):
    return broadcast_dept_names.value.get(dept_id)

emp_final = emp.withColumn("dept_name", get_dept_names(col("department_id")))

# Calculate total salary of Department 6
emp.where("department_id = 6").groupBy("department_id").agg(sum("salary").cast("long")).show()


# Accumulators
print("Using Accumulator to find the sum of salary for department_id 6")
dept_sal = spark.sparkContext.accumulator(0)

# Use foreach
def calculate_salary(department_id, salary):
    if department_id == 6:
        return dept_sal.add(salary)

emp.foreach(lambda row : calculate_salary(row.department_id, row.salary))
# View total value
print(dept_sal.value)

time.sleep(60)




