from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, expr
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType, BooleanType

# Initialize Spark session
spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# Define schema for the incoming sensor data
sensor_schema = StructType() \
    .add('stream', StringType()) \
    .add("sensor_id", StringType()) \
    .add("reading", FloatType()) \
    .add("error", BooleanType()) \
    .add("timestamp", TimestampType())

# Read the Kafka stream
sensor_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor1-data, sensor2-data") \
    .load()

# Convert Kafka's value (which is in bytes) to string and then JSON
sensor_data = sensor_stream.select(from_json(col("value").cast("string"), sensor_schema).alias("data")).select("data.*")

# Calculate latency as the difference between the current timestamp and the event timestamp
sensor_data_with_latency = sensor_data \
    .withColumn("spark_finish_time", current_timestamp()) \
    .withColumn("latency", expr("unix_timestamp(spark_finish_time) - unix_timestamp(timestamp)"))

# Perform transformations on the sensor data (e.g., filtering)
# Example: Filter rows where error is True
failed_log = sensor_data_with_latency.filter(col("error") == True)

output_path = "./spark_logs"

# Output the processed data to console (with latency included)
console_query = failed_log.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Export the processed data with latency to CSV
export_query = failed_log.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_path) \
    .option("checkpointLocation", "./spark_logs/checkpoint") \
    .start()

console_query.awaitTermination()
export_query.awaitTermination()