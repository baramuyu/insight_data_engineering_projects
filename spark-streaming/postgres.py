#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import DataFrameWriter
import os

class PostgresConnector(object):
    def __init__(self):
        self.database_name = 'occupancy'
        self.hostname = 'ec2-52-39-242-144.us-west-2.compute.amazonaws.com'
        self.url_connect = "jdbc:postgresql://{hostname}:5432/{db}".format(hostname=self.hostname, db=self.database_name)
        self.properties = {"user":"spark_user", 
                      "password":os.environ['POSTGRES_PASS'],
                      "driver": "org.postgresql.Driver"
                     }
    def set_spark(self, spark):
        self.spark = spark
        
    def _get_writer(self, df):
        return DataFrameWriter(df)
        
    def write(self, df, table, mode):
        my_writer = self._get_writer(df)
        my_writer.jdbc(self.url_connect, table, mode, self.properties)
        
    def read(self, table):
        return self.spark.read.jdbc(url=self.url_connect, table=table, properties=self.properties)
        

