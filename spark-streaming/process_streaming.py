#!/usr/bin/env python
# coding: utf-8

# In[345]:


from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter

import os

def placeholder(spark, rdd):
    
    
    database_name = 'occupancy'
    hostname = 'ec2-52-39-242-144.us-west-2.compute.amazonaws.com'
    url_connect = "jdbc:postgresql://{hostname}:5432/{db}".format(hostname=hostname, db=database_name)
    properties = {"user":"spark_user", 
                  "password":os.environ['POSTGRES_PASS'],
                  "driver": "org.postgresql.Driver"
                 }


    # In[347]:


    from collections import OrderedDict
    config_schema = OrderedDict()
    config_schema = [
        ('data_id', 'INT'),
        ('meter_id', 'INT'),
        ('transaction_id', 'INT'),
        ('transaction_timestamp', 'STRING'),
        ('amount_usd', 'INT'),
        ('usernumber', 'STRING'),
        ('payment_mean', 'STRING'),
        ('paid_duration', 'INT'),
        ('station_id', 'INT'),
        ('year', 'INT'),
        ('month', 'INT'),
        ('vendor', 'STRING'),
    ]
    schema = ", ".join(["{} {}".format(col, type) for col, type in config_schema])

    tr_df = spark.read.csv(rdd, header=False, schema=schema, mode='PERMISSIVE')

    print("tr_df.count()", tr_df.count())
    if tr_df.count() == 0:
        print("no data")
        return


    # In[349]:


    tr_df = tr_df.select('transaction_timestamp','station_id','paid_duration','amount_usd')
    tr_df = tr_df.withColumn("transaction_timestamp", F.to_timestamp(tr_df.transaction_timestamp, format="MM/dd/yyyy HH:mm:ss"))
    tr_df = tr_df.withColumn("transaction_endtime", (F.unix_timestamp("transaction_timestamp") + tr_df.paid_duration).cast('timestamp'))


    # In[350]:


    tr_df = tr_df.withColumnRenamed("transaction_timestamp", "transaction_starttime")
    tr_df = tr_df.select('station_id','transaction_starttime', 'transaction_endtime','paid_duration','amount_usd')


    # In[351]:


    table = "active_transactions"
    mode = "overwrite"
    my_writer = DataFrameWriter(tr_df)
    my_writer.jdbc(url_connect, table, mode, properties)


    # In[352]:


    tr_df.count()


    # In[353]:


    tr_df.createOrReplaceTempView("occupancy_streaming")


    # In[107]:


    tr_df.printSchema()
    tr_df.show(1)


    # In[314]:


    min_ds = spark.sql("select max(transaction_starttime) as latest_timestamp from occupancy_streaming")
    min_ds = min_ds.withColumn("latest_timestamp", F.date_trunc('minute', min_ds.latest_timestamp)) 

    min_ds = min_ds.withColumn("second_timestamp", (F.unix_timestamp(min_ds.latest_timestamp) - 60).cast("timestamp")) \
        .withColumn("third_timestamp", (F.unix_timestamp(min_ds.latest_timestamp) - 120) \
        .cast("timestamp")) \
        .collect()





    # In[336]:

    table = "dim_stations"
    dim_df = spark.read.jdbc(url=url_connect, table=table, properties=properties)
    dim_df = dim_df.select('station_id').withColumn("timestamp", F.lit(min_ds[0][0]))
    dim_df = dim_df.union(dim_df.withColumn("timestamp", F.lit(min_ds[0][1])))
    dim_df = dim_df.union(dim_df.withColumn("timestamp", F.lit(min_ds[0][2])))

    dim_df.printSchema()
    dim_df.createOrReplaceTempView("occupancy_perminute")

    sql = ("SELECT p.station_id, p.timestamp, count(*) AS occupied_spots "
           "FROM occupancy_perminute p left outer join occupancy_streaming s "
           "ON p.station_id = s.station_id "
           "AND p.timestamp BETWEEN s.transaction_starttime AND s.transaction_endtime "
           "GROUP BY p.station_id, p.timestamp"
          )

    occupancy_per_minute = spark.sql(sql)
    occupancy_per_minute.show(100)

    if occupancy_per_minute.count() > 0:
        table = "live_occupancy"
        mode = "overwrite"
        my_writer = DataFrameWriter(occupancy_per_minute)
        my_writer.jdbc(url_connect, table, mode, properties)
        
        print("updated live_occupancy !!!")
    else:
        print ("Occupancy per minute is empty")

        

def run(spark, rdd):
    placeholder(spark, rdd)
