#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import (functions as F,
                         SparkSession,
                         DataFrameWriter
                        )
import os
import postgres
from pytz import timezone
from datetime import datetime
from pyspark.sql import SparkSession

class ProcessTrans(object):
    
    def __init__(self):
        self.pgres_connector = postgres.PostgresConnector()
        self.spark = SparkSession \
        .builder \
        .appName("plops_streaming") \
        .getOrCreate()
        
    def get_schema(self):
        schema = [
            ('data_id', 'INT'),
            ('meter_id', 'INT'),
            ('transaction_id', 'INT'),
            ('transaction_timestamp', 'STRING'),
            ('amount_usd', 'DOUBLE'),
            ('usernumber', 'STRING'),
            ('payment_mean', 'STRING'),
            ('paid_duration', 'INT'),
            ('station_id', 'INT'),
            ('year', 'INT'),
            ('month', 'INT'),
            ('vendor', 'STRING'),
        ]
        return ", ".join(["{} {}".format(col, type) for col, type in schema])

    def read_csv(self, rdd):
        return self.spark.read.csv(rdd, header=False, schema=self.get_schema(), mode='PERMISSIVE')
    
    def manipulate_trans(self, df):
        df = df.select('station_id', 'paid_duration', 'transaction_timestamp')
        # convert to timestamp and calcurate endtime
        df = df.withColumn("transaction_timestamp", F.to_timestamp(df.transaction_timestamp, format="MM/dd/yyyy HH:mm:ss")) \
               .withColumn("transaction_endtime", (F.unix_timestamp("transaction_timestamp") + df.paid_duration).cast('timestamp'))
        df = df.select('transaction_endtime', 'station_id')
        return df
       
    def get_dimension_table(self):
        self.pgres_connector.set_spark(self.spark)
        dim_df = self.pgres_connector.read(table="dim_stations")
        dim_df = dim_df.select('station_id').cache()
        return dim_df
    
    def create_occupancy_df(self, dim_df):
        datetime_now = datetime.now(timezone('America/Los_Angeles')).replace(tzinfo=None)
        ocp_df = dim_df.withColumn("timestamp", F.lit(datetime_now))
        return ocp_df
    
    def calc_occupancy_per_minute(self, trans_df, ocp_df):
        trans_df.createOrReplaceTempView("transactions")
        ocp_df.createOrReplaceTempView("occupancy_perminute")

        sql = ("SELECT p.station_id, p.timestamp, CAST(count(*) as int) AS occupied_spots "
               "FROM occupancy_perminute p LEFT OUTER JOIN transactions t "
               "ON p.station_id = t.station_id "
               "AND p.timestamp < t.transaction_endtime "
               "GROUP BY p.station_id, p.timestamp"
              )
        return self.spark.sql(sql)
        
    def transform(self, rdd):
        print("*** TRANSFORM START***** ")
        print(datetime.now().isoformat())     
        
        trans_df = self.read_csv(rdd)

        trans_df = self.manipulate_trans(trans_df)
        trans_rdd = trans_df.rdd
        return trans_rdd
        
        
    def update_db(self, trans):
        print("*** UPDATE DB START***** ")
        print(datetime.now().isoformat())     

        trans_df = self.spark.createDataFrame(trans)
        dim_df = self.get_dimension_table()
        ocp_df = self.create_occupancy_df(dim_df)
        occupancy_per_min_df = self.calc_occupancy_per_minute(trans_df, ocp_df)

        if occupancy_per_min_df.count() > 0:
            table = "live_occupancy"
            mode = "overwrite"
            self.pgres_connector.write(occupancy_per_min_df, table, mode)

            print("Successfully updated live_occupancy")
            print(datetime.now().isoformat())    
        else:
            print ("Occupancy per minute data is empty")


def updateState(newValues, lastValues):
    datetime_now = datetime.now(timezone('America/Los_Angeles')).replace(tzinfo=None)
    transaction_endtime = newValues or lastValues
    if transaction_endtime[0] > datetime_now:
        return transaction_endtime
    else:
        return None # delete key
