import postgresdb
import os

def get_params():
    return {
        'dbname': 'occupancy',
        'user': 'spark_user',
        'password': os.environ['POSTGRES_PASS'],
        'host': 'ec2-52-39-242-144.us-west-2.compute.amazonaws.com'
    }

def fetchRealTimeData(lat, lng):
    params = get_params()
    pgres = postgresdb.PostgresAdapter(**params)
    try:       
        sql = ("(SELECT d.station_id, (d.space_count - lv.occupied_spots) AS available_spots, "
                "d.location_lat, d.location_lng, d.station_address, d.space_count, lv.timestamp, 1 as availability, "
                "CAST(ST_Distance(d.location_geom, ST_Transform(ST_SetSRID(ST_MakePoint({lng},{lat}),4326),2163)) * 3.28084 as int) as distance_f "
                "FROM live_occupancy lv LEFT JOIN dim_stations d "
                "ON d.station_id = lv.station_id "
                "WHERE d.space_count - lv.occupied_spots > 0 "
                "ORDER BY ST_Distance(d.location_geom, ST_Transform(ST_SetSRID(ST_MakePoint({lng},{lat}),4326),2163)) "
                "LIMIT 10 ) "
                "UNION "
                "(SELECT d.station_id, (d.space_count - lv.occupied_spots) AS available_spots, "
                "d.location_lat, d.location_lng, d.station_address, d.space_count, lv.timestamp, 0 as availability, "
                "CAST(ST_Distance(d.location_geom, ST_Transform(ST_SetSRID(ST_MakePoint({lng},{lat}),4326),2163)) * 3.28084 as int) as distance_f "
                "FROM live_occupancy lv LEFT JOIN dim_stations d "
                "ON d.station_id = lv.station_id "
                "WHERE d.space_count - lv.occupied_spots <= 0 "
                "AND ST_Distance(d.location_geom, ST_Transform(ST_SetSRID(ST_MakePoint({lng},{lat}),4326),2163)) < 400 "
                "ORDER BY ST_Distance(d.location_geom, ST_Transform(ST_SetSRID(ST_MakePoint({lng},{lat}),4326),2163)) "
                "LIMIT 10 ) "
              ).format(lng=lng, lat=lat)
        print ("sql: ",sql)
        records = pgres.execute(sql, json_format=True)
        return records
    except Exception as e:
        raise

        
def fetchHourlyData(id):
    params = get_params()
    pgres = postgresdb.PostgresAdapter(**params)
    try:       
        sql = ("SELECT station_id, hour, round(avg(occupancy),0) as occupied_spots "
                "FROM hist_occupancy "
                "WHERE station_id = ({station_id}) "
                "AND day_of_week = EXTRACT(DOW FROM current_date) "
                "GROUP BY station_id, hour "
                "ORDER BY station_id, hour "
              ).format(station_id=id)
        print ("sql: ",sql)
        records = pgres.execute(sql, json_format=True)
        return records
    except Exception as e:
        raise
        