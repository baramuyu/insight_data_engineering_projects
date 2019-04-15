from pyspark.sql import (SparkSession, 
                         functions as F,
                        )
import postgres
import argparse

DEBUG = False

class ProcessHistOccupancyData(object):
    def __init__(self, file_name):
        self.spark = SparkSession \
            .builder \
            .appName("plops_batch") \
            .getOrCreate()
        
        schema_list = [
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
        self.schema = ", ".join(["{} {}".format(col, type) for col, type in schema_list])
        self.col_select = ( "timestamp",
                            "occupancy",
                            "station_id",
                          )

        self.file_name = file_name

    def read_csv_from_s3(self):
        file_name = "s3a://project.datasets/{file_name}".format(file_name=self.file_name)
        mode = "PERMISSIVE"
        if DEBUG:
            mode = "FAILFAST"
        
        print('reading file: ' + file_name)
        return self.spark.read.csv(file_name, header=True, mode=mode, schema=self.schema)

    def manipulate_df(self, csv_df):
        df = csv_df.select(*self.col_select)
        df = df.withColumn("timestamp", F.to_timestamp(df.timestamp, format="mm/dd/yyyy hh:mm:ss a"))
        df = df.withColumn('day_of_week', F.dayofweek(df.timestamp)) \
                .withColumn('hour', F.hour(df.timestamp)) \
                .dropDuplicates(['timestamp', 'station_id'])
        return df
    
    def write_to_postgres(self, out_df):
        table = "hist_occupancy"
        mode = "append"
        
        connector = postgres.PostgresConnector()
        connector.write(out_df, table, mode)

    def run(self):
        csv_df = self.read_csv_from_s3()
        csv_df.printSchema()
        
        out_df = self.manipulate_df(csv_df)
        out_df.printSchema()
        
        self.write_to_postgres(out_df)
        
        print("Batch process finished.")

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", help="debug mode, loads small test file.", action="store_true")
    parser.add_argument("--file", help="file name to process")
    args = parser.parse_args()
    
    file_name = args.file if args.file else "2019-Paid-Parking-Occupancy.csv.gz"
    if args.debug:
        DEBUG = True 
        file_name = 'small_data/last_48h.csv.gz' # smaller file
    proc = ProcessHistOccupancyData(file_name)
    proc.run()

run()