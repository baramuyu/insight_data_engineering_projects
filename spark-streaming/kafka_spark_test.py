#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import json
from collections import OrderedDict
from pyspark.sql import functions as F

sc = pyspark.SparkContext().getOrCreate()
ssc = pyspark.streaming.StreamingContext(sc, 10)
sc.setLogLevel("ERROR") # use DEBUG when you have a problem


from pyspark.sql import SparkSession
spark = SparkSession \
.builder \
.appName("plops") \
.getOrCreate()

brokers = "10.0.0.8:9092"
topic = "paid-transaction"
n_partitions = 2
offset_0 = 0
offset_1 = 0

fromOffsets = {TopicAndPartition(topic, 1): int(offset_0),
              TopicAndPartition(topic, 0): int(offset_1)}

sqlContext = pyspark.sql.SQLContext(sc)
kafkaStream = KafkaUtils.createDirectStream(ssc, [topic],
            {"metadata.broker.list": brokers},
            fromOffsets=fromOffsets
)

print ("Start consuming datastream")

def get_schema():
    config_schema = OrderedDict()
    config_schema = [
        ('data_id', 'INT'),
        ('meter_id', 'INT'),
        ('transaction_id', 'INT'),
        ('transaction_timestamp', 'STRING'),
        ('amount_usd', 'DOUBLE'),
        ('usernumber', 'INT'),
        ('payment_mean', 'STRING'),
        ('paid_duration', 'INT'),
        ('station_id', 'INT'),
        ('year','INT'),
        ('month', 'INT'),
        ('vendor', 'STRING'),
    ]
    return ", ".join(["{} {}".format(col, type) for col, type in config_schema])

def process(rdd):
    df = spark.read.csv(rdd, header=False, schema=get_schema(), mode='FAILFAST')
    df = df.select('transaction_timestamp','station_id','paid_duration','amount_usd')
    df = df.withColumn('transaction_timestamp', F.to_timestamp(df.transaction_timestamp, format="MM/dd/yyyy HH:mm:ss"))
    df.show()

    
lines = kafkaStream.map(lambda x: x[0])
lines.pprint()
lines.foreachRDD(process)


ssc.start()
ssc.awaitTermination()

