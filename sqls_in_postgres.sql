occupancy=> \d+ spark_output_occupancy 
                                       Table "public.spark_output_occupancy"
   Column   |            Type             | Collation | Nullable | Default | Storage  | Stats target | Description 
------------+-----------------------------+-----------+----------+---------+----------+--------------+-------------
 timestamp   | timestamp without time zone |           |          |         | plain    |              | 
 occupancy  | integer                     |           |          |         | plain    |              | 
 station_id | integer                     |           |          |         | plain    |              | 
 location   | text                        |           |          |         | extended |              | 

CREATE EXTENSION postgis;
ALTER TABLE hist_occupancy ADD COLUMN location_geom geometry(POINT,2163);
ALTER TABLE hist_occupancy DROP COLUMN location;

occupancy=> \d+ hist_occupancy;
                                            Table "public.hist_occupancy"
    Column     |            Type             | Collation | Nullable | Default | Storage | Stats target | Description 
---------------+-----------------------------+-----------+----------+---------+---------+--------------+-------------
 timestamp      | timestamp without time zone |           |          |         | plain   |              | 
 occupancy     | integer                     |           |          |         | plain   |              | 
 station_id    | integer                     |           |          |         | plain   |              | 
 location_geom | geometry(Point,2163)        |           |          |         | main    |              | 



-- Spacial query
select ST_Distance(
	ST_GeomFromText('POINT (41.61350928 -122.31824513)', 4326), 
	ST_GeomFromText('POINT (47.61350928 -121.31824513)', 4326)
	);

--    st_distance    
-- ------------------
--  6.08276253029822
-- (1 row)

INSERT INTO hist_occupancy 
SELECT timestamp, occupancy, station_id, ST_GeomFromText(location, 2163) 
FROM spark_output_occupancy;


SELECT create_hypertable('hist_occupancy', 'timestamp', chunk_time_interval => interval '30 day');