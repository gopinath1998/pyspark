import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


logger.info("Starting PySpark job")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Logging in Spark Application") \
    .getOrCreate()

logger.info("Spark session created")

try:
    # Read input data
    input_path = "s3://your-bucket/input-data/"
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    logger.info(f"Data read from {input_path}")

    # Process data
    processed_df = df.filter(col("age") > 18)
    logger.info("Data filtered")

    # Write output data
    output_path = "s3://your-bucket/output-data/"
    processed_df.write.parquet(output_path, mode="overwrite")
    logger.info(f"Data written to {output_path}")

except Exception as e:
    logger.error(f"Error during processing: {e}", exc_info=True)
finally:
    spark.stop()
    logger.info("Spark session stopped")
