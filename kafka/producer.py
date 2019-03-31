from datetime import datetime
from kafka.producer import KafkaProducer
import pytz
import random
import time

'''
Generate real-time transaction data per second and publish to kafka topic.

schema from Seattle.gov
"DataId,MeterCode,TransactionId,TransactionDateTime,Amount,UserNumber,PaymentMean,PaidDuration,ElementKey,TransactionYear,TransactionMonth,Vendor"
[28085294,10508004,685518869,03/24/2019 23:39:54,2,NULL,CREDIT CARD,7200,81454,2019,3]
'''

class GenerateStreaming():
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.increment = 0
        self.pacific_time = pytz.timezone('America/Los_Angeles')
        self.fmt = '%Y/%m/%d %H:%M:%S'
        
        
    def generate(self):
        DataId = self.increment
        MeterCode = random.choice([7098002, 17143004, 13035002, 18004007, 19294002, 1114002, 10282004, 10348004, 5040102,
                     18005007, 24001004, 10345002, 19343010, 8052002, 21091004, 21043004, 18016007, 7039002, 12152304])
        TransactionId = self.increment
        TransactionDateTime =datetime.now(self.pacific_time).strftime(self.fmt)
        Amount = random.choice([0.25, 0.5,1,1.5,2,3])
        PaymentMean = random.choice(['CREDIT CARD', 'PHONE', 'CASH'])
        PaidDuration = random.randint(1, 10800)
        ElementKey = MeterCode
        data_send = ",".join(map(str,[DataId,MeterCode,TransactionId,TransactionDateTime,Amount,'',
                                      PaymentMean,PaidDuration,ElementKey,'2019','4','']))
        print (data_send)
        self.increment += 1
        return data_send

    def send(self):
        self.producer.send('my-topic', self.generate().encode())
    
def __main__():
    limit = 0
    datagen = GenerateStreaming()
    while limit < 10:
        datagen.send()
        limit += 1
        time.sleep(1)