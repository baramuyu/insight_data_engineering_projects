#!/usr/bin/env python
# coding: utf-8

import csv
import time
import operator
from pytz import timezone
from datetime import datetime, timedelta
from kafka.producer import KafkaProducer

PACIFIC_TIME = timezone('America/Los_Angeles')
producer = KafkaProducer(bootstrap_servers='10.0.0.8:9092,10.0.0.5:9092')

class Producer(object):
    def __init__(self):
        #self.producer = KafkaProducer(bootstrap_servers='10.0.0.8:9092,10.0.0.5:9092')
        pass
        
    def sort_and_update_trans_csv(self, filename):
        # read and sort
        with open(filename, 'r') as csvfile:
            trans = csv.reader(csvfile)
            next(trans)
            sorted_trans = sorted(trans, key=operator.itemgetter(3))

        # write back
        with open(filename, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(sorted_trans)

    def lazy_read_trans_csv(self, filename):
        with open(filename) as f:
            rows = csv.reader(f)
            next(rows, None)
            for row in rows:
                yield row

    def create_data_to_send(self, row):
        ElementKey = row[8]
        data_send = ",".join(map(str, row))
        print(data_send)
        key = str(ElementKey).encode()
        value = data_send.encode()
        
        return key, value

    def send_to_kafka(self, row):
        producer.send('paid-transaction', *self.create_data_to_send(row))
    
    def wait_until_tomorrow(self):
        now = datetime.now(PACIFIC_TIME).replace(tzinfo=None) 
        tomorrow = now + timedelta(days=1)
        midnight = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, 
                            hour=0, minute=1, second=0)
        print('wait until ' + midnight.strftime("%Y-%m-%d %H:%M:%S"))
        time.sleep((midnight - now).seconds)
    
    def run(self, filename):
        self.sort_and_update_trans_csv(filename)
        
        for row in self.lazy_read_trans_csv(filename):
            if len(row) < 12:
                continue
            # convert 7 days ago to today
            trans_time = datetime.strptime(row[3], "%m/%d/%Y %H:%M:%S") + timedelta(days=7)
            row[3] = trans_time.strftime("%m/%d/%Y %H:%M:%S")
            printed = False
            # compare to the transaction time and current_time of 7 days ago
            while trans_time > datetime.now(PACIFIC_TIME).replace(tzinfo=None):
                if not printed:
                    print('next', row[3])
                    printed = True
                time.sleep(5)
            self.send_to_kafka(row)

            
if __name__ == "__main__":
    while True:
        lastweek = datetime.now(PACIFIC_TIME).replace(tzinfo=None) - timedelta(days=7)
        filename = lastweek.strftime('%m%d%Y') + '.csv'
        print('reading: ' + filename)
        prod = Producer()
        prod.run(filename)
        prod.wait_until_tomorrow()

