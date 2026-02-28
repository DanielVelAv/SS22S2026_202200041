
USE practica1_ss2;
GO

-- 1. Total de vuelos por aerolinea
SELECT a.nombre AS aerolinea, COUNT(*) AS total_vuelos
FROM fact_vuelo f
JOIN dim_aerolinea a ON f.id_aerolinea = a.id_aerolinea
GROUP BY a.nombre
ORDER BY total_vuelos DESC;

-- 2. Top 5 destinos mas frecuentes
SELECT TOP 5 ap.codigo AS destino, COUNT(*) AS total_vuelos
FROM fact_vuelo f
JOIN dim_aeropuerto ap ON f.id_aeropuerto_destino = ap.id_aeropuerto
GROUP BY ap.codigo
ORDER BY total_vuelos DESC;

-- 3. Distribucion de pasajeros por genero
SELECT p.genero, COUNT(*) AS total_pasajeros
FROM fact_vuelo f
JOIN dim_pasajero p ON f.id_pasajero = p.id_pasajero
GROUP BY p.genero
ORDER BY total_pasajeros DESC;

-- 4. Vuelos por estado (ON_TIME, DELAYED, CANCELLED, DIVERTED)

SELECT e.estado, COUNT(*) AS total_vuelos
FROM fact_vuelo f
JOIN dim_estado_vuelo e ON f.id_estado = e.id_estado
GROUP BY e.estado
ORDER BY total_vuelos DESC;


-- 5. Promedio de retraso por aerolinea (solo vuelos con retraso)

SELECT a.nombre AS aerolinea, 
       ROUND(AVG(f.retraso_min), 2) AS promedio_retraso
FROM fact_vuelo f
JOIN dim_aerolinea a ON f.id_aerolinea = a.id_aerolinea
WHERE f.retraso_min > 0
GROUP BY a.nombre
ORDER BY promedio_retraso DESC;


-- 6. Ingresos totales por moneda

SELECT m.moneda, 
       ROUND(SUM(f.precio_ticket), 2) AS total_ingresos,
       COUNT(*) AS cantidad_tickets
FROM fact_vuelo f
JOIN dim_moneda m ON f.id_moneda = m.id_moneda
GROUP BY m.moneda
ORDER BY total_ingresos DESC;


-- 7. Metodo de pago mas utilizado

SELECT mp.metodo, COUNT(*) AS total_usos
FROM fact_vuelo f
JOIN dim_metodo_pago mp ON f.id_metodo = mp.id_metodo
GROUP BY mp.metodo
ORDER BY total_usos DESC;

-- 8. Top 5 rutas mas frecuentes (origen -> destino)

SELECT TOP 5
    ao.codigo AS origen,
    ad.codigo AS destino,
    COUNT(*) AS total_vuelos
FROM fact_vuelo f
JOIN dim_aeropuerto ao ON f.id_aeropuerto_origen = ao.id_aeropuerto
JOIN dim_aeropuerto ad ON f.id_aeropuerto_destino = ad.id_aeropuerto
GROUP BY ao.codigo, ad.codigo
ORDER BY total_vuelos DESC;


-- 9. Cantidad de vuelos por mes y anio

SELECT t.anio, t.mes, COUNT(*) AS total_vuelos
FROM fact_vuelo f
JOIN dim_tiempo t ON f.id_tiempo_salida = t.id_tiempo
GROUP BY t.anio, t.mes
ORDER BY t.anio, t.mes;

-- 10. Canal de venta mas utilizado
SELECT c.canal, COUNT(*) AS total_reservas
FROM fact_vuelo f
JOIN dim_canal_venta c ON f.id_canal = c.id_canal
GROUP BY c.canal
ORDER BY total_reservas DESC;


-- 11. Promedio de precio en USD por clase de cabina

SELECT cl.clase, 
       ROUND(AVG(f.precio_usd), 2) AS precio_promedio_usd
FROM fact_vuelo f
JOIN dim_clase_cabina cl ON f.id_clase = cl.id_clase
GROUP BY cl.clase
ORDER BY precio_promedio_usd DESC;

-- 12. Distribucion de pasajeros por nacionalidad

SELECT p.nacionalidad, COUNT(*) AS total_pasajeros
FROM fact_vuelo f
JOIN dim_pasajero p ON f.id_pasajero = p.id_pasajero
WHERE p.nacionalidad IS NOT NULL
GROUP BY p.nacionalidad
ORDER BY total_pasajeros DESC;


-- 13. Promedio de equipaje por canal de venta

SELECT c.canal,
       ROUND(AVG(CAST(f.equipaje_total AS FLOAT)), 2) AS promedio_equipaje
FROM fact_vuelo f
JOIN dim_canal_venta c ON f.id_canal = c.id_canal
GROUP BY c.canal
ORDER BY promedio_equipaje DESC;

-- 14. Vuelos cancelados por aerolinea
SELECT a.nombre AS aerolinea, COUNT(*) AS cancelados
FROM fact_vuelo f
JOIN dim_aerolinea a ON f.id_aerolinea = a.id_aerolinea
JOIN dim_estado_vuelo e ON f.id_estado = e.id_estado
WHERE e.estado = 'CANCELLED'
GROUP BY a.nombre
ORDER BY cancelados DESC;

-- 15. Tipo de avion mas utilizado
SELECT av.tipo, COUNT(*) AS total_vuelos
FROM fact_vuelo f
JOIN dim_tipo_avion av ON f.id_tipo_avion = av.id_tipo_avion
GROUP BY av.tipo
ORDER BY total_vuelos DESC;
