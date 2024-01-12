from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.window import Window

import pyhdfs
import json

# Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("Stock Analysis") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .config("spark.mongodb.output.uri", "mongodb://root:admin@mongodb:27017/bigdata.stock2024") \
    .getOrCreate()

# Setup the HDFS client
hdfs = pyhdfs.HdfsClient(hosts="namenode:9870", user_name="hdfs")
directory = '/data'
files = hdfs.listdir(directory)
print("Files in '{}':".format(directory), files)

# Define the schema for the DataFrame
schema = StructType([
    StructField("stock", StringType(), True),
    StructField("date", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

# Function to create a DataFrame from a file's content
def create_dataframe_from_file(file_path):
    try:
        # Read the file content
        file_content = hdfs.open(file_path).read().decode('utf-8')
        # Convert JSON content to Python dictionary
        data = json.loads(file_content)
        # Create a DataFrame using the defined schema
        return spark.createDataFrame([data], schema)
    except Exception as e:
        print("Failed to read '{}': {}".format(file_path, e))
        return None

# Create an empty DataFrame with the specified schema
df = spark.createDataFrame([], schema)

# Iterate over files and create DataFrame
for file in files:
    file_path = "{}/{}".format(directory, file)
    file_df = create_dataframe_from_file(file_path)
    if file_df:
        file_df = file_df.withColumn("date", to_timestamp(file_df["date"], 'yyyy-MM-dd\'T\'HH:mm:ss'))
        df = df.unionByName(file_df)

from pyspark.sql import functions as F

# Remove duplicates
df = df.dropDuplicates()

# Convert date from StringType to TimestampType and sort
df = df.withColumn("date", F.to_timestamp("date"))
df = df.orderBy("stock", "date")

# Basic Statistics for each stock
basic_stats = df.groupBy("stock").agg(
    F.mean("open").alias("avg_open"),
    F.mean("high").alias("avg_high"),
    F.mean("low").alias("avg_low"),
    F.mean("close").alias("avg_close"),
    F.mean("volume").alias("avg_volume"),
    F.stddev("close").alias("std_dev_close"),
    F.max("high").alias("historical_high"),
    F.min("low").alias("historical_low")
)

daily_window_spec = Window.partitionBy("stock").orderBy("date")

# Calculate daily returns
df = df.withColumn("prev_day_close", F.lag("close").over(daily_window_spec))
df = df.withColumn("daily_return", (F.col("close") - F.col("prev_day_close")) / F.col("prev_day_close"))

# Calculate 1-day moving average
df = df.withColumn("moving_avg_1d", F.avg("close").over(daily_window_spec.rowsBetween(0, 0)))

# Show results
basic_stats.show()
df.select("stock", "date", "daily_return", "moving_avg_1d").show()

# Truncate time to date
df = df.withColumn("date_only", F.to_date("date"))

# Determine daily opening and closing prices for each stock
daily_prices = df.groupBy("stock", "date_only").agg(
    F.first("open").alias("daily_open"),
    F.last("close").alias("daily_close")
)

# Add a column to indicate daily change for each stock: 1 for increase, -1 for decrease, 0 for no change
daily_prices = daily_prices.withColumn("daily_change", F.when(F.col("daily_close") > F.col("daily_open"), 1).when(F.col("daily_close") < F.col("daily_open"), -1).otherwise(0))

# Group by date and aggregate to count increases, decreases, and unchanged for all stocks
daily_change_stats = daily_prices.groupBy("date_only").agg(
    F.sum(F.when(F.col("daily_change") == 1, 1).otherwise(0)).alias("num_stocks_increased"),
    F.sum(F.when(F.col("daily_change") == -1, 1).otherwise(0)).alias("num_stocks_decreased"),
    F.sum(F.when(F.col("daily_change") == 0, 1).otherwise(0)).alias("num_stocks_unchanged")
)

# Show results
daily_change_stats.show()

# Example of writing the basic_stats DataFrame to MongoDB
basic_stats.write.format("mongo").mode("append").save()
# Similarly for other DataFrames
daily_change_stats.write.format("mongo").mode("append").save()


from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# K-Means Model
vec_assembler = VectorAssembler(inputCols=["avg_open", "avg_high", "avg_low", "avg_close", "avg_volume"], outputCol="features")
df_kmeans = vec_assembler.transform(basic_stats)

kmeans = KMeans().setK(3).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(df_kmeans)

predictions = model.transform(df_kmeans)
predictions.select("stock", "prediction").show()

