# Proyecto 2 - Seminario de Sistemas 2


### 1. Dataset y Limpieza de Datos
Se utilizó el dataset público `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`. Dado su tamaño (~40GB), se aplicó un filtro en la cláusula \`WHERE\` para procesar únicamente los primeros tres meses del 2022 logrando el requerimiento mínimo de registros sin comprometer el coste computacional.
Se aplicó la siguiente **Limpieza y Transformación**:
- \`fare_amount > 0\`: Se eliminaron viajes con tarifas irreales o devoluciones.
- \`trip_distance > 0\`: Se descartaron registros registrados por sistema que no representaron un viaje físico real.
- \`tip_amount >= 0\`: Se sanearon anomalías negativas en propinas.
- \`passenger_count > 0 AND passenger_count IS NOT NULL\`: Se garantizó estructura válida para las predicciones posteriores eliminado nulos estructurales.

### 2. Optimización (Particiones y Clustering)
Se generó una tabla derivada intermedia: \`taxi_trips_2022_optimizada\`.
- **PARTITION BY:** Se particionó utilizando \`DATE(pickup_datetime)\`.
- **CLUSTER BY:** Se indexó físicamente por \`pickup_location_id\` y \`dropoff_location_id\`.
- **Reducción lograda:** Al evaluar la carga de escaneo desde el validador estático, se evidencia una disminución crítica en el cobro de escaneo garantizando optimización a nivel nube: La consulta cruda proyectaba **996.84 MB** mientras que la consulta a la tabla optimizada proyectaba únicamente **66.62 MB**. 

![Comparación de Escaneo Estático](Documentacion/1.ConsultaCrudaBytes.png)

En los Detalles de la ejecución posterior, se observa que la consulta sin optimizar procesa el doble de registros y consume 12.18 KB de bytes generados/movidos con 12.48 s de tiempo de ranura, frente a los 5.9 KB y 1.49 s de tiempo de ranura del modelo optimizado y particionado.

**Detalles de Ejecución - Consulta Cruda**
![Ejecución original cruda](Documentacion/1.ConsultaCruda.png)

**Detalles de Ejecución - Consulta Optimizada**
![Ejecución optimizada particionada](Documentacion/detalles_ejecucion_optimizada.png)

### 3. Modelo Supervisado y Análisis predictivo
**Problema a resolver**: Predecir financieramente la tarifa aproximada de un viaje particular.
- **Tipo:** Modelo Supervisado - \`linear_reg\` (Regresión Lineal Múltiple).
- **Variable Objetivo (Salida):** \`fare_amount\`.
- **Variables de Entrada (Features):** \`trip_distance\`, \`pickup_location_id\`, \`dropoff_location_id\`, \`passenger_count\`.
- **Ingeniería de Características:** Se derivaron \`pickup_hour\` (Hora del día) y \`day_of_week\` (Día de la semana) explícitamente de la consulta para capturar comportamiento cíclico del tráfico.
- **Manejo de Data Leakage:** Se utilizó \`data_split_method='RANDOM'\` destinando estrictamente un fraccionamiento (\`data_split_eval_fraction=0.2\`) garantizando un 80% Train y 20% Evaluate separados completamente a nivel kernel de BigQuery.

### 4. Tuning de Hiperparámetros y Modelo Seleccionado
Se corrieron dos variantes del algoritmo entrenando las variables base:
* **M1_Base**: Configuraciones iniciales base.
* **M2_Tuned**: Tuneo de \`l1_reg=0.2\`, \`l2_reg=0.01\` y \`ls_init_learn_rate=0.1\`.

**Justificación del Modelo Seleccionado:**
Al cruzar \`ML.EVALUATE\` entre ambas tablas logramos comparar las métricas. El M2 demostró dominar por amplio margen a nivel precisión:
* MSE (Error Cuadrático Medio) del M1: ~1,239,048.29
* MSE (Error Cuadrático Medio) del M2: ~43.05

Al mitigar los "outliers" de la regresión y reducir la penalización del ruido usando Regularización L1, el modelo M2 es el *Modelo Final* indiscutible asigado al proceso \`ML.PREDICT\`.

**Resultados ML.EVALUATE: M1 vs M2**
![Resultados M1 vs M2](Documentacion/evaluacion_m1_m2_resultados.png)

### 5. Modelo Secundario (No Supervisado) - Clustering
**Problema a resolver**: Encontrar agrupaciones (clústeres) naturales en los viajes de taxi según características de distancia, tarifa, propina y tiempo, para comprender los distintos perfiles de los usuarios y viajes.
- **Tipo:** Modelo No Supervisado - `kmeans` (K-Means Clustering).
- **Variable Objetivo:** Ninguna (por su naturaleza no supervisada).
- **Variables de Entrada (Features):** `trip_distance`, `fare_amount`, `tip_amount`, `trip_duration_min`, `pickup_hour`.
- **Evaluación y Modelos:** Se crearon dos versiones del modelo con distintos hiperparámetros:
  - **M3 (Base):** K-Means con `num_clusters=3`.
  - **M4 (Optimizado):** K-Means escalado a `num_clusters=5`, agregando la variable de tiempo (`pickup_hour`) e implementando inicialización inteligente `kmeans_init_method='KMEANS_PLUS_PLUS'`.
- **Modelo Seleccionado:** Tras evaluar el índice `davies_bouldin_index` con `ML.EVALUATE`, el modelo M4 (5 clusters) fue elegido para el tablero, ya que sus 5 grupos permiten una granularidad mayor para perfilar los tipos de viajes (ej. viajes cortos de baja tarifa vs viajes largos de aeropuerto).

> Modelos M3 y M4. métrica `davies_bouldin_index`, Se eligió el modelo con el mejor índice.

> Ejecución de query
![Evaluaciones K-Means M3 vs M4](Documentacion/evaluacion_kmeans_m3_m4.PNG)


> Evaluaciones K-Means M3 vs M4
![Evaluaciones K-Means M3 vs M4](Documentacion/evaluacion_kmeans_m3_m4_1.PNG)

### 6. Insights y Hallazgos Visuales (Dashboard)
A través de Looker Studio se generaron visualizaciones exploratorias conectadas dinámicamente a las tablas de BigQuery. Los hallazgos más destacados fueron:
1. **Picos de Demanda:** Se identificó mediante un gráfico de barras que la mayor densidad de viajes ocurre hacia el final de la tarde (17:00 - 19:00), coincidiendo con los horarios de salida laboral.
2. **Zonas Rentables:** El análisis visual con mapas de calor/burbujas revela que ciertas zonas de recogida (zonas aeroportuarias o distritos céntricos) generan los `fare_amount` promedios más altos.
3. **Distribución:** La representación en el gráfico de anillo validó el amplio dominio de ciertos métodos de pago, lo cual orienta estrategias de cobro.
4. **Panel de Machine Learning:** 
   - **Rolling Forecast (Regresión vs Realidad):** Se visualizó mediante un gráfico de serie temporal la comparativa progresiva entre las tarifas reales (`tarifa_real`) frente a las tarifas predichas por el modelo de Regresión Lineal Múltiple (`tarifa_predicha`) a lo largo de las distintas horas de recogida.
   - **Clústeres (K-Means):** Se incorporó una gráfica de burbujas usando el identificador `CENTROID_ID_TEXT` para categorizar visualmente a los 5 perfiles (clústeres) descubiertos por el modelo M4, observando de forma clara su agrupación por precio y distancia.


> Exploratorio
![Dashboard Exploratorio](Documentacion/dashboard_exploratorio.PNG)

> Modelos ML
![Dashboard Machine Learning](Documentacion/dashboard_modelos_ml.PNG)
