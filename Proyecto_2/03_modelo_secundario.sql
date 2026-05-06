
-- ==============================================================================
-- MODELO SECUNDARIO (M3 y M4): K-MEANS CLUSTERING (No Supervisado)
-- ESTUDIANTE 2: LOURDES
-- ==============================================================================

-- IMPORTANTE: Este modelo K-Means es NO SUPERVISADO.
-- A diferencia del modelo de tu compañero, este NO TIENE VARIABLE OBJETIVO (label).
-- Las variables de entrada (Features) son las únicas que alimentan al modelo 
-- y su propósito es descubrir patrones/agrupaciones ocultas en los datos.

-- Entrenamiento Modelo Base M3 (Sin variable objetivo)
CREATE OR REPLACE MODEL `Conjunto1.modelo_kmeans_v1`
OPTIONS(
  model_type='kmeans',
  num_clusters=3, -- Hiperparámetro principal
  standardize_features=TRUE
) AS
SELECT 
  trip_distance, 
  fare_amount, 
  passenger_count,
  TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration_min
FROM `Conjunto1.taxi_trips_2022_optimizada`;


-- Entrenamiento Modelo Optimizado M4 (Distintos hiperparámetros)
-- Agregamos la hora (para ver demanda temporal), cambiamos a 5 grupos y mejoramos la inicialización.
CREATE OR REPLACE MODEL `Conjunto1.modelo_kmeans_v2`
OPTIONS(
  model_type='kmeans',
  num_clusters=5, -- Segundo hiperparámetro
  standardize_features=TRUE,
  kmeans_init_method='KMEANS_PLUS_PLUS'
) AS
SELECT 
  trip_distance, 
  fare_amount,
  tip_amount,
  TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration_min,
  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour
FROM `Conjunto1.taxi_trips_2022_optimizada`;


-- Comparación y Asignación
-- Evaluar qué modelo quedó mejor configurado analizando el davies_bouldin_index
SELECT 'M3_Base (3 Clusters)' AS modelo, * FROM ML.EVALUATE(MODEL `Conjunto1.modelo_kmeans_v1`);
SELECT 'M4_Optimizado (5 Clusters)' AS modelo, * FROM ML.EVALUATE(MODEL `Conjunto1.modelo_kmeans_v2`);

-- Generar la tabla de predicciones (Asignación de Clúster) que luego consumirás en Looker Studio.
-- Se crea la columna CENTROID_ID_TEXT para evitar problemas de tipos de datos en la gráfica de dispersión.
CREATE OR REPLACE TABLE `Conjunto1.predictions_kmeans` AS
SELECT 
  CONCAT('Cluster ', CAST(CENTROID_ID AS STRING)) AS CENTROID_ID_TEXT,
  * 
FROM ML.PREDICT(MODEL `Conjunto1.modelo_kmeans_v2`, 
  (SELECT 
    *,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration_min,
    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour
   FROM `Conjunto1.taxi_trips_2022_optimizada`)
);
