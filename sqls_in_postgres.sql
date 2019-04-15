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

GRANT select ON hist_occupancy, live_occupancy, dim_stations TO spark_user, web_server;
GRANT select ON live_occupancy TO spark_user, web_server;

SELECT create_hypertable('hist_occupancy', 'timestamp', chunk_time_interval => interval '7 day');

// dim_stations;
ALTER TABLE dim_stations ADD COLUMN location_geom geometry(POINT,2163);
UPDATE dim_stations SET location_geom = ST_Transform(ST_SetSRID(ST_MakePoint(location_lng, location_lat),4326),2163);

insert into dim_stations select distinct * from dim_stations2;


// history
SELECT d.station_address, round(avg(h.occupancy),1) AS average_occupancy, d.space_count, d.location, h.station_id 
FROM hist_occupancy h LEFT JOIN dim_stations d on h.station_id = d.station_id 
WHERE h.station_id = 34214 
AND h.timestamp between '2019-01-18 13:00:00'::timestamp AND '2019-01-18 14:00:00'::timestamp 
GROUP BY h.station_id, d.space_count, d.location, d.station_address


/// 10 Closest available spots
SELECT d.station_id, (d.space_count - lv.occupied_spots) AS available_spots, d.location_lat, d.location_lng, d.station_address, d.space_count,
CAST(ST_Distance(d.location_geom, ST_Transform(ST_SetSRID(ST_MakePoint(-122.3326,47.6027),4326),2163)) as int) as distance_m
FROM live_occupancy lv LEFT JOIN dim_stations d
ON d.station_id = lv.station_id
WHERE d.space_count - lv.occupied_spots > 0
ORDER BY ST_Distance(d.location_geom, ST_Transform(ST_SetSRID(ST_MakePoint(-122.3326,47.6027),4326),2163)) 
LIMIT 10


// TRIGGERS for live_occupancy

CREATE UNIQUE INDEX index_station_id ON live_occupancy (station_id);

CREATE OR REPLACE FUNCTION update_live_data()
  RETURNS trigger AS
$BODY$
BEGIN
 INSERT INTO live_occupancy
 VALUES(NEW.station_id, NEW.occupied_spots, NEW.timestamp) ON CONFLICT (station_id)  
 DO UPDATE SET occupied_spots = NEW.occupied_spots, timestamp = NEW.timestamp;
 DELETE FROM spark_out_live_occupancy where station_id = NEW.station_id;
 
 RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;


CREATE TRIGGER trigger_update_live_data
  BEFORE INSERT
  ON spark_out_live_occupancy
  FOR EACH ROW
  EXECUTE PROCEDURE update_live_data();

// hist_occupancy setup

SELECT create_hypertable('hist_occupancy', 'timestamp', 'station_id', 4);
CREATE INDEX ON hist_occupancy (station_id, timestamp DESC);

CREATE OR REPLACE FUNCTION update_hist_data()
  RETURNS trigger AS
$BODY$
BEGIN
 INSERT INTO hist_occupancy
 VALUES(NEW.timestamp, NEW.occupancy, NEW.station_id, NEW.day_of_week, NEW.hour) 
 ON CONFLICT (timestamp, station_id) DO NOTHING;
 DELETE FROM spark_out_hist_occupancy 
 WHERE station_id = NEW.station_id AND timestamp = NEW.timestamp;
 
 RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;


CREATE TRIGGER trigger_update_hist_data
  BEFORE INSERT
  ON spark_out_hist_occupancy
  FOR EACH ROW
  EXECUTE PROCEDURE update_hist_data();


// Setting of timescaleDB
SELECT create_hypertable('spark_out_hist_occupancy', 'timestamp', 'station_id', chunk_time_interval => interval '7 day');
SELECT set_chunk_time_interval('spark_out_hist_occupancy', interval '7 day');
SELECT set_chunk_time_interval('hist_occupancy', interval '7 day');

