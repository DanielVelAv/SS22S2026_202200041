-- Modelo multidimensional estrella

CREATE DATABASE practica1_ss2;
GO
USE practica1_ss2;
GO

-- DIMENSIONES


-- Dimension: Aerolinea
CREATE TABLE dim_aerolinea (
    id_aerolinea INT IDENTITY(1,1) PRIMARY KEY,
    codigo VARCHAR(5) NOT NULL,
    nombre VARCHAR(100) NOT NULL
);

-- Dimension: Aeropuerto
CREATE TABLE dim_aeropuerto (
    id_aeropuerto INT IDENTITY(1,1) PRIMARY KEY,
    codigo VARCHAR(5) NOT NULL
);

-- Dimension: Pasajero
CREATE TABLE dim_pasajero (
    id_pasajero INT IDENTITY(1,1) PRIMARY KEY,
    passenger_id VARCHAR(50) NOT NULL,
    genero VARCHAR(2),
    edad INT,
    nacionalidad VARCHAR(5)
);

-- Dimension: Tiempo
CREATE TABLE dim_tiempo (
    id_tiempo INT IDENTITY(1,1) PRIMARY KEY,
    fecha DATE NOT NULL,
    anio INT,
    mes INT,
    dia INT,
    trimestre INT
);

-- Dimension: Canal de venta
CREATE TABLE dim_canal_venta (
    id_canal INT IDENTITY(1,1) PRIMARY KEY,
    canal VARCHAR(50) NOT NULL
);

-- Dimension: Metodo de pago
CREATE TABLE dim_metodo_pago (
    id_metodo INT IDENTITY(1,1) PRIMARY KEY,
    metodo VARCHAR(50) NOT NULL
);

-- Dimension: Estado del vuelo
CREATE TABLE dim_estado_vuelo (
    id_estado INT IDENTITY(1,1) PRIMARY KEY,
    estado VARCHAR(20) NOT NULL
);

-- Dimension: Tipo de avion
CREATE TABLE dim_tipo_avion (
    id_tipo_avion INT IDENTITY(1,1) PRIMARY KEY,
    tipo VARCHAR(20) NOT NULL
);

-- Dimension: Clase de cabina
CREATE TABLE dim_clase_cabina (
    id_clase INT IDENTITY(1,1) PRIMARY KEY,
    clase VARCHAR(30) NOT NULL
);

-- Dimension: Moneda
CREATE TABLE dim_moneda (
    id_moneda INT IDENTITY(1,1) PRIMARY KEY,
    moneda VARCHAR(5) NOT NULL
);

-- TABLA DE HECHOS

CREATE TABLE fact_vuelo (
    id_fact INT IDENTITY(1,1) PRIMARY KEY,
    record_id INT NOT NULL,
    flight_number VARCHAR(20),
    seat VARCHAR(10),

    -- FKs a dimensiones
    id_aerolinea INT FOREIGN KEY REFERENCES dim_aerolinea(id_aerolinea),
    id_aeropuerto_origen INT FOREIGN KEY REFERENCES dim_aeropuerto(id_aeropuerto),
    id_aeropuerto_destino INT FOREIGN KEY REFERENCES dim_aeropuerto(id_aeropuerto),
    id_pasajero INT FOREIGN KEY REFERENCES dim_pasajero(id_pasajero),
    id_tiempo_salida INT FOREIGN KEY REFERENCES dim_tiempo(id_tiempo),
    id_tiempo_llegada INT FOREIGN KEY REFERENCES dim_tiempo(id_tiempo),
    id_tiempo_reserva INT FOREIGN KEY REFERENCES dim_tiempo(id_tiempo),
    id_canal INT FOREIGN KEY REFERENCES dim_canal_venta(id_canal),
    id_metodo INT FOREIGN KEY REFERENCES dim_metodo_pago(id_metodo),
    id_estado INT FOREIGN KEY REFERENCES dim_estado_vuelo(id_estado),
    id_tipo_avion INT FOREIGN KEY REFERENCES dim_tipo_avion(id_tipo_avion),
    id_clase INT FOREIGN KEY REFERENCES dim_clase_cabina(id_clase),
    id_moneda INT FOREIGN KEY REFERENCES dim_moneda(id_moneda),

    -- Medidas
    duracion_min FLOAT,
    retraso_min FLOAT,
    precio_ticket FLOAT,
    precio_usd FLOAT,
    equipaje_total INT,
    equipaje_documentado INT
);
