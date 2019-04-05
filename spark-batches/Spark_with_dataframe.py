#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark = SparkSession .builder .appName("plops") .getOrCreate()


# In[2]:


from collections import OrderedDict
config_schema = OrderedDict()
config_schema = [
    ('timestamp', 'string'),
    ('occupancy', 'INT'),
    ('blockface_name', 'STRING'),
    ('side_of_street', 'STRING'),
    ('station_id', 'INT'),
    ('time_limit_category', 'STRING'),
    ('space_count', 'INT'),
    ('area', 'STRING'),
    ('subarea', 'STRING'),
    ('rate', 'STRING'),
    ('category', 'STRING'),
    ('location', 'STRING'),
    ('emptycol1','STRING'),
    ('emptycol2','STRING'),
    ('emptycol3','STRING'),
    ('emptycol4','STRING'),
    ('emptycol5','STRING')
]


# In[3]:


schema = ", ".join(["{} {}".format(col, type) for col, type in config_schema])
schema


# In[4]:


df = spark.read.csv(
    #"s3a://project.datasets/Last_48_hours.csv", header=True, mode="DROPMALFORMED", schema=schema
    #"s3a://project.datasets/last_48h.csv.gz", header=True, mode="DROPMALFORMED", schema=schema
    "s3a://project.datasets/2019-Paid-Parking-Occupancy.csv.gz", header=True, mode="DROPMALFORMED", schema=schema
)


# In[5]:


df.schema


# In[6]:


df = df.drop("blockface_name", 
             "side_of_street", 
             "time_limit_category", 
             "space_count", 
             "area", 
             "subarea",
             "rate",
             "category",
             "emptycol1",
             "emptycol2",
             "emptycol3",
             "emptycol4",
             "emptycol5"
            )


# In[7]:


df.printSchema()


# In[8]:


from pyspark.sql import functions as F


# In[9]:


df = df.withColumn("timestamp", F.to_timestamp(df.timestamp, format="mm/dd/yyyy hh:mm:ss a"))


# In[10]:


df.printSchema()


# In[11]:


df = df.withColumn('day_of_week', F.dayofweek(df.timestamp))
df = df.withColumn('hour', F.hour(df.timestamp))


# In[12]:


#Create New DataFrame
# from pyspark.sql.types import (StructType,
#                                TimestampType,
#                                IntegerType,
#                                DoubleType)
# field = [
#     StructField('DateTime', TimestampType(), True),
#     StructField('StationID', StringType(), True),
#     StructField('AveOpenSpots', IntegerType(), True),
#     StructField('AveOpenRate', DoubleType(), True),
#     StructField('GroupByPeriod', StringType(), True),
#     StructField('Location', StringType(), True)
# ]
# new_schema = StructType(field)
# df_ave = sqlContext.createDataFrame(sc.emptyRDD(), new_schema)
# df_ave.show()


# In[13]:


df.printSchema()


# In[14]:


import os
from pyspark.sql import DataFrameWriter
my_writer = DataFrameWriter(df)


# In[15]:


database_name = 'occupancy'
hostname = 'ec2-52-39-242-144.us-west-2.compute.amazonaws.com'
url_connect = "jdbc:postgresql://{hostname}:5432/{db}".format(hostname=hostname, db=database_name)

table = "spark_output_occupancy"
mode = "overwrite"
properties = {"user":"spark_user", 
              "password":os.environ['POSTGRES_PASS'],
              "driver": "org.postgresql.Driver"
             }


# In[ ]:


#my_writer.jdbc(url_connect, table, mode, properties)


# In[ ]:




