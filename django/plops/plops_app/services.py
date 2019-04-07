import postgresdb
import os

def fetchData():
    params = {
        'dbname': 'occupancy',
        'user': 'web_server',
        'password': os.environ['POSTGRES_PASS'],
        'host': 'ec2-52-39-242-144.us-west-2.compute.amazonaws.com'
    }
    pgres = postgresdb.PostgresAdapter(**params)
    #pgres = PostgresAdapter(**params)
    try:
        sql = ("select d.station_address, round(avg(h.occupancy),1) as average_occupancy, d.space_count, d.location, h.station_id " 
                "from hist_occupancy h left join dim_stations d on h.station_id = d.station_id "
                "where h.station_id = 34214 "
                "and h.timestamp between '2019-01-18 13:00:00'::timestamp AND '2019-01-18 14:00:00'::timestamp "
                "group by h.station_id, d.space_count, d.location, d.station_address ")
        records = pgres.execute(sql, json_format=True)
        sql = ("select d.station_address, h.occupied_spots, d.space_count, d.location, h.station_id "
                "from live_occupancy h left join dim_stations d on h.station_id = d.station_id "
                "where h.station_id = 13261 "
                "and h.timestamp = '2019-04-05 12:51:00'::timestamp ")
        hist_records = pgres.execute(sql, json_format=True)
        records += hist_records
        
        for rec in records:
            print('rec: ',rec)
            # extract lat and long from "POINT (47.60580762 -122.33341762)" 
            loc = rec['location'].replace("(","").replace(")","").split()[1:]
            rec['location_lat'] = float(loc[0])
            rec['location_lng'] = float(loc[1])
            del rec['location']
        
        return records
    except Exception as e:
        raise
        