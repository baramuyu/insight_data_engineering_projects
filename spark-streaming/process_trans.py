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
        df = df.select('station_id', 'transaction_endtime')
        return df
       
    def get_all_station_ids(self, sparkContext):
        def _fetch_dimension_table():
            self.pgres_connector.set_spark(self.spark)
            dim_df = self.pgres_connector.read(table="dim_stations")
            return dim_df.select('station_id').collect()
        
        # get from global variables, if doesn't exist, get from DB.
        if ("dim_stations" not in globals()):
            dim_df = _fetch_dimension_table()
            globals()["dim_stations"] = sparkContext.broadcast(dim_df) 
            
        dim_rows = globals()["dim_stations"]
        return self.spark.createDataFrame(dim_rows.value)
    
    def create_occupancy_df(self, dim_df):
        datetime_now = datetime.now(timezone('America/Los_Angeles')).replace(tzinfo=None)
        all_stations_df = dim_df.withColumn("timestamp", F.lit(datetime_now))
        return all_stations_df
    
    def join_occupancy_df_with_all_stations(self, occupancy_df, all_stations_df):
        occupancy_df.createOrReplaceTempView("occupancies")
        all_stations_df.createOrReplaceTempView("all_stations")

        sql = ("SELECT s.station_id, coalesce(o.occupied_spots, 0) AS occupied_spots, s.timestamp "
               "FROM all_stations s LEFT JOIN occupancies o "
               "ON s.station_id = o.station_id "
              )
        return self.spark.sql(sql)
    
    def update_output_table(self, live_df):
        table = "spark_out_live_occupancy"
        mode = "append"
        self.pgres_connector.write(live_df, table, mode)

        print("Successfully updated live_occupancy")
        print(datetime.now().isoformat())  
        
    def transform(self, rdd):
        print("*** TRANSFORM START***** ")
        print(datetime.now().isoformat())     
        
        trans_df = self.read_csv(rdd)
        trans_df = self.manipulate_trans(trans_df)
        trans_rdd = trans_df.rdd
        return trans_rdd
        
    def update_db(self, occupancy_rdd):
        print("*** UPDATE DB START***** ")
        print(datetime.now().isoformat())   
        
        occupancy_df = self.spark.createDataFrame(occupancy_rdd)
        dim_df = self.get_all_station_ids(occupancy_rdd.context)
        all_stations_df = self.create_occupancy_df(dim_df)
        live_df = self.join_occupancy_df_with_all_stations(occupancy_df, all_stations_df)
        
        if live_df.count() > 0:
            self.update_output_table(live_df)
