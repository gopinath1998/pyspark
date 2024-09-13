from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("SnowflakeMergeExample") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.1") \
    .getOrCreate()

# Snowflake connection properties
sfOptions = {
    "sfURL": "your_snowflake_account_url",
    "sfAccount":"AccountIdentifier",
    "sfUser": "your_snowflake_username",
    "sfPassword": "your_snowflake_password",
    "sfDatabase": "your_database",
    "sfSchema": "your_schema",
    "sfWarehouse": "your_warehouse",
    "sfRole": "your_role"  # Optional
}

# Read the target table from Snowflake
target_table = "your_target_table"
target_df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", target_table) \
    .load()

# Create or load the source DataFrame to be merged
# Here we create a simple DataFrame for illustration
data = [("1", "John", "Doe", 30), ("2", "Jane", "Doe", 25)]
columns = ["id", "first_name", "last_name", "age"]
source_df = spark.createDataFrame(data, columns)

# Write the source DataFrame to a temporary Snowflake table
temp_table = "temp_merge_table"
source_df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .option("dbtable", temp_table) \
    .mode("overwrite") \
    .save()

# Perform the merge operation using Snowflake SQL
merge_sql = f"""
MERGE INTO {sfOptions['sfSchema']}.{target_table} AS target
USING {sfOptions['sfSchema']}.{temp_table} AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET target.first_name = source.first_name,
               target.last_name = source.last_name,
               target.age = source.age
WHEN NOT MATCHED THEN
    INSERT (id, first_name, last_name, age)
    VALUES (source.id, source.first_name, source.last_name, source.age)
"""

# Execute the merge statement
sf_connector = spark._sc._jvm.net.snowflake.spark.snowflake.Utils
sf_connector.runQuery(sfOptions, merge_sql)
# The code sf_connector = spark._sc._jvm.net.snowflake.spark.snowflake.
# Utils is used to access the Snowflake utility class within the Snowflake Spark connector
# from the Java Virtual Machine (JVM) through PySpark.
# This allows you to execute Snowflake SQL commands directly from PySpark.


# Clean up temporary table
drop_temp_table_sql = f"DROP TABLE {sfOptions['sfSchema']}.{temp_table}"
sf_connector.runQuery(sfOptions, drop_temp_table_sql)

# Stop the Spark session
spark.stop()
