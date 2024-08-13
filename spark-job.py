
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col

spark = SparkSession.builder.appName("IoTDataProcessing").getOrCreate()

# Read data from S3
input_path = "s3://iot-sensor-data-bucket/"
df = spark.read.json(input_path)
df.show()

# Data transformation
df = df.filter((col('temperature') > 0) & (col('temperature') < 50))
df = df.withColumn('timestamp', from_unixtime(col('timestamp')))
df = df.groupBy("sensor_id").agg(
    avg("temperature").alias("avg_temperature"),
    max("humidity").alias("max_humidity"),
    min("temperature").alias("min_temperature")
)
df.show()

# Write the result back to S3
output_path = "s3://iot-processed-data-bucket/"
df.write.mode("overwrite").format("json").save(output_path)

print("Data processing and writing to S3 completed successfully.")


spark.stop()
