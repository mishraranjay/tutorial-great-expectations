 # Code generated via "Slingshot" 
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, sha2, concat_ws, year, month, dayofmonth
from pyspark.sql.types import StringType

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("AWS Glue Job") \
    .getOrCreate()

# Input and output path configuration
input_bucket = "s3://your-input-bucket/raw/current_date/input"
output_bucket = "s3://your-output-bucket/consumption/current_date/output"

# Read CSV file from S3 as DataFrame
df = spark.read.csv(input_bucket, header=True, inferSchema=True)

# Normalize columns: convert to lowercase, replace spaces/special characters with underscores
df = df.toDF(*[c.lower().replace(' ', '_').replace('.', '_') for c in df.columns])

# Data Quality Checks: Handle nulls, remove duplicates
df = df.fillna({'string_column': 'unknown', 'number_column': 0, 'date_column': '1970-01-01'})
df = df.dropDuplicates()

# Transformations
# Add modified timestamp
df = df.withColumn("modified_ts", current_timestamp())

# Convert date_of_purchase to date type and extract year, month, day
df = df.withColumn("date_of_purchase", col("date_of_purchase").cast("date"))
df = df.withColumn("year", year(col("date_of_purchase")))
df = df.withColumn("month", month(col("date_of_purchase")))
df = df.withColumn("day", dayofmonth(col("date_of_purchase")))

# Create SHA ID
df = df.withColumn("sha_id", sha2(concat_ws("||", *df.columns), 256).cast(StringType()))

# Write to S3 with _cleaned suffix and ensure single file
df.coalesce(1).write.mode("overwrite").csv(output_bucket + "_cleaned", header=True)

# Optional: Error handling and logging
try:
    # Main processing logic
    pass
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
finally:
    spark.stop()
