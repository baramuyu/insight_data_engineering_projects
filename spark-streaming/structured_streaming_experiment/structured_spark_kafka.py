import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,TimestampType

spark = SparkSession \
.builder \
.appName("plops_streaming") \
.getOrCreate()

# actually works?
spark.sparkContext.setLogLevel("ERROR")

brokers = "10.0.0.8:9092"
topic = "paid-transaction"

# schema = StructType()\
#     .add('post',StringType())\
#     .add('subreddit',StringType())\
#     .add('timestamp',TimestampType())


input_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", brokers) \
  .option("subscribe", topic) \
  .option("startingOffsets", "earliest") \
  .load()


input_df.printSchema()

# trans_df = input_df.selectExpr("CAST(value AS STRING)")
# trans_df.show()

consoleOutput = input_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
# consoleOutput.awaitTermination()


spark.streams.awaitAnyTermination()


#df.show()


# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", brokers) \
#   .option("subscribe", topic) \
#   .option("startingOffsets", "earliest") \
#   .load()