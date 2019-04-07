#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import (functions as F,
                         SparkSession,
                         DataFrameWriter
                        )
import os
import postgres
from datetime import datetime


class ProcessTrans(object):
    
    def __init__(self, spark, rdd):
        self.spark = spark
        self.rdd = rdd
        self.pgres_connector = postgres.PostgresConnector()
        
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

    def read_csv(self):
        return self.spark.read.csv(self.rdd, header=False, schema=self.get_schema(), mode='PERMISSIVE')
    
    def manipulate_trans(self, df):
        df = df.select('station_id','paid_duration','amount_usd','transaction_timestamp')
        # convert to timestamp and calcurate endtime
        df = df.withColumn("transaction_timestamp", F.to_timestamp(df.transaction_timestamp, format="MM/dd/yyyy HH:mm:ss")) \
               .withColumn("transaction_endtime", (F.unix_timestamp("transaction_timestamp") + df.paid_duration).cast('timestamp'))
        # rename
        df = df.withColumnRenamed("transaction_timestamp", "transaction_starttime")
        # re-order
        df = df.select('station_id','transaction_starttime', 'transaction_endtime','paid_duration','amount_usd')
        return df
    
    def get_lastest_three_minutes(self, trans_df):
        trans_df.createOrReplaceTempView("occupancy_streaming")
        # get latest timestamp within the batch
        min_df = self.spark.sql("select max(transaction_starttime) as latest_timestamp from occupancy_streaming")
        # get latest minute hh:10:00, one min before; hh:09:00, two mins before; hh:08:00
        min_df = min_df.withColumn("latest_timestamp", F.date_trunc('minute', min_df.latest_timestamp)) 
        min_df = min_df.withColumn("second_timestamp", (F.unix_timestamp(min_df.latest_timestamp) - 60).cast("timestamp")) \
            .withColumn("third_timestamp", (F.unix_timestamp(min_df.latest_timestamp) - 120) \
            .cast("timestamp")) \
            .collect()
        return min_df
   
    def get_dimension_table(self, min_df):
        self.pgres_connector.set_spark(self.spark)
        dim_df = self.pgres_connector.read(table="dim_stations")
        dim_df = dim_df.select('station_id')
        return dim_df
    
    def create_occupancy_df(self, min_df, dim_df):
        latest_min = min_df[0][0]
        second_latest_min = min_df[0][1]
        third_latest_min = min_df[0][2]
        ocp_df = dim_df.withColumn("timestamp", F.lit(latest_min))
        ocp_df = ocp_df.union(dim_df.withColumn("timestamp", F.lit(second_latest_min)))
        ocp_df = ocp_df.union(dim_df.withColumn("timestamp", F.lit(third_latest_min)))

        ocp_df.printSchema()
        return ocp_df
    
    def calc_occupancy_per_minute(self, ocp_df):
        ocp_df.createOrReplaceTempView("occupancy_perminute")

        sql = ("SELECT p.station_id, p.timestamp, count(*) AS occupied_spots "
               "FROM occupancy_perminute p LEFT OUTER JOIN occupancy_streaming s "
               "ON p.station_id = s.station_id "
               "AND p.timestamp BETWEEN s.transaction_starttime AND s.transaction_endtime "
               "GROUP BY p.station_id, p.timestamp"
              )
        return self.spark.sql(sql)
        
    def run(self):
        print("*** PROCESS START***** ")
        print(datetime.now().isoformat())     

        trans_df = self.read_csv()
        print("trans_df.count()", trans_df.count())
        if trans_df.count() == 0:
            print("no data")
            return

        trans_df = self.manipulate_trans(trans_df)
        min_df = self.get_lastest_three_minutes(trans_df)
        dim_df = self.get_dimension_table(min_df)
        ocp_df = self.create_occupancy_df(min_df, dim_df)
        occupancy_per_min_df = self.calc_occupancy_per_minute(ocp_df)

        if occupancy_per_min_df.count() > 0:
            table = "live_occupancy"
            mode = "overwrite"
            self.pgres_connector.write(occupancy_per_min_df, table, mode)

            print("Successfully updated live_occupancy")
            print(datetime.now().isoformat())    
        else:
            print ("Occupancy per minute data is empty")