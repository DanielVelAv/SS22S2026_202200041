# Documentación Técnica y Estratégica - Práctica 2
**Seminario de Sistemas 2 - Diseño de Dashboard y KPIs con Power BI**

## 1. Conexiones
Para la elaboración de este dashboard, la ingesta de datos se realizó conectando Microsoft Power BI Desktop al motor de base de datos **SQL Server** desplegado previamente en un contenedor Docker (Práctica 1).
* **Servidor:** `localhost,1433`
* **Base de Datos:** `practica1_ss2`
* **Modo de Conectividad de Datos:** Importar (Import Mode)
* **Autenticación:** Credenciales de base de datos (Usuario: `sa`).

## 2. Tablas y Modelo de Datos
El modelo se construyó bajo un esquema de **Modelo Estrella (Star Schema)**, lo cual optimiza el rendimiento en Power BI.
* **Tabla de Hechos (Fact Table):** `fact_vuelo`, la cual almacena las métricas transaccionales (retraso en minutos, precio del ticket, etc.) y llaves foráneas.
* **Tablas de Dimensión (Dimensions):** `dim_aerolinea`, `dim_aeropuerto`, `dim_tiempo`, `dim_pasajero`, `dim_estado_vuelo`, `dim_tipo_avion`, `dim_clase_cabina`, `dim_canal_venta`, `dim_metodo_pago` y `dim_moneda`.
* **Relación y Cardinalidad:** Se establecieron relaciones de "Uno a varios" (1:*) desde las claves primarias de cada dimensión hacia las claves foráneas de `fact_vuelo`. La dirección del filtro cruzado es "Única".

## 3. Jerarquías Creadas
Para permitir análisis granulares (*drill-down*), se implementaron jerarquías en la dimensión temporal.
* **Jerarquía de Tiempo en `dim_tiempo`:** Se estructuró de la forma **Año > Trimestre > Mes > Día**. Esto permite a las gráficas (como la de Ingresos Mensuales) desglosar el comportamiento financiero desde una vista macro hasta una operativa diaria.

## 4. Fórmulas DAX Creadas
Se diseñaron medidas DAX explícitas en una tabla dedicada de Medidas, centralizando la lógica de negocio:
1. `Total Vuelos = COUNTROWS('fact_vuelo')`
2. `Ingreso Total USD = SUM('fact_vuelo'[precio_usd])`
3. `Vuelos Puntuales = CALCULATE([Total Vuelos], 'fact_vuelo'[retraso_min] <= 15)`
4. `Porcentaje Puntualidad = DIVIDE([Vuelos Puntuales], [Total Vuelos], 0)`
5. `Promedio de Retraso = AVERAGE('fact_vuelo'[retraso_min])`

## 5. Diseño del Dashboard, Filtros e Interactividad
El dashboard fue diseñado para ofrecer una experiencia ejecutiva (Dark/Light template) con interacciones cruzadas que responden en tiempo real:
* **Panel Lateral (Oscuro):** Contiene los segmentadores universales (Filtros) como listas desplegables.
  * **Filtros aplicados:** Año, Aerolínea y Canal de Venta.
* **Panel Central (Claro):** Utiliza un diseño en formato "Z". A la izquierda las **Tarjetas Resumen** métricas (Porcentaje Puntualidad, Total Vuelos, Ingreso Total, Promedio de Retraso) y a la derecha **3 Visualizaciones Clave**:
  1. *Vuelos por Aerolínea:* Gráfico de barras horizontales.
  2. *Ingresos Mensuales:* Gráfico de líneas.
  3. *Estados de los Vuelos:* Gráfico de anillo segmentado por colores lógicos (Verde=A tiempo, Rojo=Cancelado, Azul oscuro=Retrasado). Todos interactúan al 100% entre sí al seleccionar un dato.

## 6. Configuración Estratégica de KPIs (Perspectivas Balanced Scorecard)
Los indicadores implementados en el dashboard mapean las cuatro perspectivas fundamentales del **Balanced Scorecard (BSC)**:

###  6.1 Perspectiva: Procesos Internos
* **KPI Operacional:** Porcentaje de Puntualidad (Indicador Semáforo).
* **Objetivo:** Maximizar la eficiencia de los vuelos minimizando el índice de despegues y aterrizajes impuntuales.
* **Métrica / Fórmula:** `[Porcentaje Puntualidad]`
* **Umbrales (Semáforo):**
  *  **Verde (Óptimo):** >= 85%
  *  **Amarillo (Alerta temprana):** 75% - 84%
  * **Rojo (Crítico / Deficiente):** < 75%
* **Responsable:** Gerente de Operaciones y Logística.
* **Interpretación:** Actualmente se mantiene un KPI del **80.87%** (Amarillo), indicando procesos funcionales pero que requieren optimización para reducir el retraso promedio de 26.07 minutos.

###  6.2 Perspectiva: Financiera
* **KPI Financiero:** Ingresos Totales Mensuales.
* **Objetivo:** Sostener una curva de ingresos mensual predecible que garantice la rentabilidad de las rutas.
* **Métrica / Fórmula:** `[Ingreso Total USD]` (Gráfico temporal).
* **Umbrales:** Observación de tendencia. Se monitorean caídas bruscas.
* **Responsable:** Dirección Financiera (CFO).
* **Interpretación:** La línea mensual muestra caídas drásticas en ciertos meses, fuertemente correlacionadas con altos índices de cancelación según los cruces interactivos.

###  6.3 Perspectiva: Clientes
* **KPI de Satisfacción (Indirecta):** Tasa de Cumplimiento de Rutas.
* **Objetivo:** Mitigar fricciones en la experiencia del viajero monitoreando el nivel de vuelos cancelados/desviados.
* **Métrica / Fórmula:** Ratio en anillo de vuelos `ON_TIME` vs `CANCELLED`/`DELAYED`.
* **Responsable:** Dirección de Servicio al Cliente.
* **Interpretación:** Las cancelaciones (5.6%) y los retrasos (19.7%) representan más del 25% de la operación, generando insatisfacción profunda que debe abordarse reduciendo la porción roja/naranja del KPI visual.

###  6.4 Perspectiva: Aprendizaje y Crecimiento
* **KPI Tecnológico:** Adopción Organizacional e Infraestructura de Canales.
* **Objetivo:** Fomentar el uso de canales modernos y evaluar capacitación de equipos de ventas.
* **Métrica:** Volumen de vuelos gestionados filtrados por `Canal de Venta`.
* **Responsable:** Dirección comercial e IT.
* **Interpretación:** El filtro "Canal de Venta" permite hacer cruces A/B instantáneos para detectar si ciertos canales de venta están asociados con mayores problemas operativos, indicando potenciales áreas de capacitación técnica.
