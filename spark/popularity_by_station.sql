CREATE OR REPLACE VIEW station_popularity AS
SELECT
    start_station_name,
    COUNT(*) AS trips_started
FROM trips
GROUP BY start_station_name;
