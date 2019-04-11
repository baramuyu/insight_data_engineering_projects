#!/usr/bin/env python
# coding: utf-8

from datetime import datetime
from kafka.producer import KafkaProducer
import pytz
import random
import time
import json

producer = KafkaProducer(bootstrap_servers='10.0.0.8:9092,10.0.0.5:9092')

# get Pacific timezone
pacific_time = pytz.timezone('America/Los_Angeles')
fmt = '%m/%d/%Y %H:%M:%S'
pacific_dt = pacific_time.localize(datetime.utcnow())

# schema from Seattle.gov
# "DataId,MeterCode,TransactionId,TransactionDateTime,Amount,UserNumber,PaymentMean,PaidDuration,ElementKey,TransactionYear,TransactionMonth,Vendor"
# [28085294,10508004,685518869,03/24/2019 23:39:54,2,NULL,CREDIT CARD,7200,81454,2019,3]

class GenerateData():
    def __init__(self):
        self.increment = 0
        
    def station_list(self):
        with open('pay_stations_list.txt', 'r') as f:
            station_list = f.read().split()
            station_list = map(int, station_list)
        return station_list
    
    def run(self):
        DataId = self.increment
        MeterCode = random.choice(self.station_list())
        TransactionId = self.increment
        TransactionDateTime =datetime.now(pacific_time).strftime(fmt)
        Amount = random.choice([0.25, 0.5,1,1.5,2,3])
        PaymentMean = random.choice(['CREDIT CARD', 'PHONE', 'CASH'])
        max_duration_sec = 60 * 60 * 3
        PaidDuration = random.randint(1, max_duration_sec)
        ElementKey = MeterCode
        record = [DataId,MeterCode,TransactionId,TransactionDateTime,Amount,'',
                    PaymentMean,PaidDuration,ElementKey,'2019','4','']
        data_send = ",".join(map(str, record))
        
        self.increment += 1
        
        print (data_send)
        
        key = str(MeterCode).encode()
        value = data_send.encode()
        return key, value
    
limit = 0
datagen = GenerateData()
while True:
    producer.send('paid-transaction', *datagen.run())
    limit += 1
    time.sleep(.5)





