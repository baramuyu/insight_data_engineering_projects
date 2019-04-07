#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import (functions as F,
                         SparkSession,
                         SQLContext
                        )

from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

import json
import process_trans

class KafkaConsumer(object):
    def __init__(self):
        batchDuration = 10
        self.sec_window = 180
        self.sec_slide = 20
        
        self.sc = SparkContext().getOrCreate()
        self.ssc = StreamingContext(self.sc, batchDuration)
        self.sc.setLogLevel("ERROR") # use DEBUG when you have a problem
        
        self.spark = SparkSession \
        .builder \
        .appName("plops_streaming") \
        .getOrCreate()
        
        self.conf = self.get_kafka_consumer_setting()

    def get_kafka_consumer_setting(self):
        return {'brokers': '10.0.0.8:9092',
                     'topic': 'paid-transaction',
                     'n_partitions': 2,
                     'offset_0': 0,
                     'offset_1': 0, 
                    }
        
    def get_kafkaStream(self):
        return KafkaUtils.createDirectStream(
                        self.ssc, 
                        [self.conf['topic']],
                        {"metadata.broker.list": self.conf['brokers']},
                        fromOffsets=self.get_kafka_offsets(**self.conf)
                     )
        
    def get_kafka_offsets(self, topic, offset_0, offset_1, **kwargs):
        fromOffsets = {TopicAndPartition(topic, 1): int(offset_0),
                       TopicAndPartition(topic, 0): int(offset_1)}
        return fromOffsets
        


    def run(self):
        def _process_rdd(rdd):
            process_trans.ProcessTrans(self.spark, rdd).run()
            
        print ("Start consuming datastream")

        kafkaStream = self.get_kafkaStream()
        lines = kafkaStream.window(self.sec_window, self.sec_slide)\
                           .map(lambda x: x[0]) # retrieve value
        lines.pprint()
        lines.foreachRDD(_process_rdd)

        self.ssc.start()
        self.ssc.awaitTermination()


consumer = KafkaConsumer()
consumer.run()