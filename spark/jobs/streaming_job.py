from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("user_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("location", StringType()) \
    .add("timestamp", DoubleType())

# Read
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transaction_events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert binary → string
json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Rule 1: High amount
fraud_df = parsed_df.filter(col("amount") > 4000)

query = fraud_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()