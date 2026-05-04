
-- 1. Exploración Inicial
SELECT 
  COUNT(*) as total_viajes,
  AVG(fare_amount) as tarifa_promedio,
  AVG(trip_distance) as distancia_promedio
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`;

-- 2. Creación de la Tabla Intermedia Optimizada
-- Particionamos por fecha de inicio del viaje y clusterizamos por las ubicaciones, 

CREATE OR REPLACE TABLE `seminariosistemas2-495301.Conjunto1.taxi_trips_2022_optimizada`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY pickup_location_id, dropoff_location_id
AS
SELECT 
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_location_id,
  dropoff_location_id,
  fare_amount,
  tip_amount,
  total_amount
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
WHERE fare_amount > 0             
  AND trip_distance > 0           
  AND tip_amount >= 0             
  AND passenger_count > 0         
  AND passenger_count IS NOT NULL 
  AND EXTRACT(YEAR FROM pickup_datetime) = 2022
  AND EXTRACT(MONTH FROM pickup_datetime) <= 3;

-- 3. Comprobación de la optimización 
-- Consulta a tabla cruda:
SELECT pickup_location_id, AVG(fare_amount) 
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022` 
WHERE DATE(pickup_datetime) BETWEEN '2022-01-01' AND '2022-01-31'
GROUP BY pickup_location_id;

-- Consulta a tu tabla optimizada
SELECT pickup_location_id, AVG(fare_amount) 
FROM `seminariosistemas2-495301.Conjunto1.taxi_trips_2022_optimizada` 
WHERE DATE(pickup_datetime) BETWEEN '2022-01-01' AND '2022-01-31'
GROUP BY pickup_location_id;
