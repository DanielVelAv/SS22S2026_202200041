
-- 1. Entrenamiento Modelo Base M1
-- TIPO: Supervisado (Regresión Lineal)
-- Variable Objetivo: fare_amount
-- Variables de Entrada: passenger_count, trip_distance, pickup_location_id, dropoff_location_id, pickup_hour, day_of_week
-- Ingeniería de Características: Se derivaron pickup_hour y day_of_week desde pickup_datetime para atrapar patrones de tráfico.
CREATE OR REPLACE MODEL `seminariosistemas2-495301.Conjunto1.modelo_tarifa_m1`
OPTIONS(
  model_type='linear_reg',
  input_label_cols=['fare_amount'],
  data_split_method='RANDOM', -- Manejo de Data Leakage (80% train, 20% test)
  data_split_eval_fraction=0.2 
) AS
SELECT
  IFNULL(passenger_count, 1) AS passenger_count,
  trip_distance,
  CAST(pickup_location_id AS STRING) AS pickup_location_id, -- Casteado a string ,para tratarlo como categoría
  CAST(dropoff_location_id AS STRING) AS dropoff_location_id,
  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
  EXTRACT(DAYOFWEEK FROM pickup_datetime) AS day_of_week,
  fare_amount
FROM `seminariosistemas2-495301.Conjunto1.taxi_trips_2022_optimizada`
  WHERE DATE(pickup_datetime) BETWEEN '2022-01-01' AND '2022-01-07'
LIMIT 10000; 


-- 2. Entrenamiento Modelo Optimizado M2 (Distintos hiperparámetros)
-- TIPO: Supervisado (Regresión Lineal)
-- Variable Objetivo: fare_amount
-- Variables de Entrada: passenger_count, trip_distance, pickup_location_id, dropoff_location_id, pickup_hour, day_of_week
CREATE OR REPLACE MODEL `seminariosistemas2-495301.Conjunto1.modelo_tarifa_m2`
OPTIONS(
  model_type='linear_reg',
  input_label_cols=['fare_amount'],
  data_split_method='RANDOM',
  data_split_eval_fraction=0.2,
  l1_reg=0.2,            -- Hiperparámetros
  l2_reg=0.01,           
  ls_init_learn_rate=0.1 
) AS
SELECT
  IFNULL(passenger_count, 1) AS passenger_count,
  trip_distance,
  CAST(pickup_location_id AS STRING) AS pickup_location_id,
  CAST(dropoff_location_id AS STRING) AS dropoff_location_id,
  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
  EXTRACT(DAYOFWEEK FROM pickup_datetime) AS day_of_week,
  fare_amount
FROM `seminariosistemas2-495301.Conjunto1.taxi_trips_2022_optimizada`
  WHERE DATE(pickup_datetime) BETWEEN '2022-01-01' AND '2022-01-07' 
LIMIT 10000;


-- 3. Comparación para Modelo Final
-- mean_squared_error
SELECT 'M1_Base' AS modelo, * FROM ML.EVALUATE(MODEL `seminariosistemas2-495301.Conjunto1.modelo_tarifa_m1`)
UNION ALL
SELECT 'M2_Tuned' AS modelo, * FROM ML.EVALUATE(MODEL `seminariosistemas2-495301.Conjunto1.modelo_tarifa_m2`);


-- 4. Predicción Final
CREATE OR REPLACE TABLE `seminariosistemas2-495301.Conjunto1.predicciones_tarifa` AS
SELECT
  trip_distance,
  fare_amount AS tarifa_real,
  predicted_fare_amount AS tarifa_predicha,
  ABS(fare_amount - predicted_fare_amount) AS margen_error,
  pickup_hour
FROM ML.PREDICT(
  MODEL `seminariosistemas2-495301.Conjunto1.modelo_tarifa_m2`,
  (
    -- datos "nuevos"
    SELECT
      IFNULL(passenger_count, 1) AS passenger_count,
      trip_distance,
      CAST(pickup_location_id AS STRING) AS pickup_location_id,
      CAST(dropoff_location_id AS STRING) AS dropoff_location_id,
      EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
      EXTRACT(DAYOFWEEK FROM pickup_datetime) AS day_of_week,
      fare_amount
    FROM `seminariosistemas2-495301.Conjunto1.taxi_trips_2022_optimizada`
    WHERE DATE(pickup_datetime) BETWEEN '2022-03-01' AND '2022-03-31' -- Evaluamos con datos de marzo (que sí existen en la tabla optimizada)
    LIMIT 5000
  )
);
