from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, to_timestamp, lit, from_unixtime
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/frauddb") \
        .option("dbtable", "fraud_transactions") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

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
parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(from_unixtime(col("timestamp")))
)

# Rule 1: High amount
high_amount_df = parsed_df.filter(col("amount") > 4000)

# Rule 2: Multiple transactions in short time
velocity_df = parsed_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        col("user_id"),
        window(col("timestamp"), "30 seconds")
    ) \
    .agg(count("*").alias("txn_count")) \
    .filter(col("txn_count") > 3)

# Joining results

high_amount_flag_df = high_amount_df.select(
    col("user_id"),
    col("amount"),
    lit(None).cast("int").alias("txn_count"),
    col("timestamp")
).withColumn("fraud_type", lit("HIGH_AMOUNT"))

velocity_flag_df = velocity_df.select(
    col("user_id"),
    lit(None).cast("double").alias("amount"),
    col("txn_count"),
    col("window").getField("start").alias("timestamp")
).withColumn("fraud_type", lit("HIGH_VELOCITY"))

final_fraud_df = high_amount_flag_df.unionByName(velocity_flag_df)

query = final_fraud_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()