#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import (functions as F,
                         SparkSession,
                         SQLContext,
                         Row
                        )

from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

import json
from process_trans import ProcessTrans

from pytz import timezone
from datetime import datetime


class KafkaConsumer(object):
    def __init__(self):
        batchDuration = 20
        
        self.sc = SparkContext().getOrCreate()
        self.sc.setLogLevel("ERROR") # use DEBUG when you have a problem

        self.ssc = StreamingContext(self.sc, batchDuration)
        self.ssc.checkpoint("hdfs://10.0.0.11:9000/checkpoint")
        self.conf = self.get_kafka_consumer_setting()
        
    def get_kafka_consumer_setting(self):
        return {'brokers': '10.0.0.8:9092, 10.0.0.5:9092',
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
        def _updateState(newValues, lastValues):
            datetime_now = datetime.now(timezone('America/Los_Angeles')).replace(tzinfo=None)

            transaction_endtime = (newValues or []) + (lastValues or [])
            if transaction_endtime:
                return [endtime for endtime in transaction_endtime if endtime and endtime > datetime_now]
            else:
                return None # delete key
        
        def _transform_rdd(rdd):
            if rdd:
                return ProcessTrans().transform(rdd)
            else:
                print ("no data")
            
        def _update_db(rdd):
            if rdd.count() > 0:
                ProcessTrans().update_db(rdd)

        print ("Start consuming datastream")

        kafkaStream = self.get_kafkaStream()
        trans = kafkaStream.map(lambda x: x[0]) # retrieve value

        trans = trans.transform(_transform_rdd)
        state = trans.map(lambda trans: (trans[0], trans[1])).updateStateByKey(_updateState)
        
        state.pprint()
        Occupancies = Row('station_id', 'occupied_spots')
        state.map(lambda x: Occupancies(x[0], len(x[1]))).foreachRDD(_update_db)

        self.ssc.start()
        self.ssc.awaitTermination()
        
consumer = KafkaConsumer()
consumer.run()