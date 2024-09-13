from pyspark.sql import SparkSession

# Initialize a Spark session
spark = (SparkSession.builder \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.1") \
    ## TO connect to SF we need jars
    .appName("SnowflakeReadExample").getOrCreate())

# Snowflake connection properties to read from a SF table
sf_read_Options = {
    "sfURL": "your_snowflake_account_url",
    "sfAccount":"AccountIdentifier",
    "sfUser": "your_snowflake_username",
    "sfPassword": "your_snowflake_password",
    "sfDatabase": "your_database",
    "sfSchema": "your_schema",
    "sfWarehouse": "your_warehouse",
    "sfRole": "your_role"  # Optional
}

# Table name in Snowflake
src_table_name = "your_table_name"

# Read data from Snowflake table into a Spark DataFrame
sf_src_df = spark.read \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_read_Options) \
    .option("dbtable", src_table_name) \
    .load()

sf_src_df.show()

# Snowflake connection properties to write into a SF table
sf_write_Options = {
    "sfURL": "your_snowflake_account_url",
    "sfAccount":"AccountIdentifier",
    "sfUser": "your_snowflake_username",
    "sfPassword": "your_snowflake_password",
    "sfDatabase": "your_database",
    "sfSchema": "your_schema",
    "sfWarehouse": "your_warehouse",
    "sfRole": "your_role"  # Optional
}

# Table name in Snowflake
trg_table_name = "your_target_table_name"

# Write data to Snowflake table
sf_src_df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sf_write_Options) \
    .option("dbtable", trg_table_name) \
    .mode("append") \
    .save()

# Stop the Spark session
spark.stop()
