-- Partitioning and clustering can help queries run faster by reducing the amount of data that needs to be scanned.

CREATE OR REPLACE TABLE `dezoomcamp-409909.citibike_data.partitioned_and_clustered_fact_rides`
PARTITION BY DATE(started_at)
CLUSTER BY start_station_id AS
SELECT * FROM `dezoomcamp-409909.citibike_data.fact_citibike_rides`;
