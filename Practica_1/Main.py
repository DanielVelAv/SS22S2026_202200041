import pandas as pd
import numpy as np

# EXTRACCION
print("EXTRACCION")

df1 = pd.read_csv('./Archivos/Dataset1.csv', sep=',', encoding='utf-8')
df2 = pd.read_csv('./Archivos/Dataset2.csv', sep=';', encoding='utf-8')

print(f"Dataset1: {len(df1)} registros, {len(df1.columns)} columnas")
print(f"Dataset2: {len(df2)} registros, {len(df2.columns)} columnas")

# TRANSFORMACION

print("\n")
print("\n")
print("FASE 2: TRANSFORMACION")

# parsear fechas con formatos mixtos
def parsear_fechas(series):
    s = series.astype(str).str.strip().replace({'': np.nan, 'nan': np.nan, 'None': np.nan})
    try:
        parsed = pd.to_datetime(s, format='mixed', dayfirst=True, errors='coerce')
    except TypeError:
        parsed = pd.to_datetime(s, dayfirst=True, errors='coerce')

    # Fallback con dateutil para los que no se parsearon
    if parsed.isna().any():
        from dateutil import parser
        mask = parsed.isna() & s.notna()
        def intentar_parseo(texto):
            try:
                return parser.parse(texto, dayfirst=True)
            except Exception:
                try:
                    return parser.parse(texto, dayfirst=False)
                except Exception:
                    return pd.NaT
        parsed.loc[mask] = pd.to_datetime(s.loc[mask].map(intentar_parseo), errors='coerce')
    return parsed

# DATASET 1 - Transformaciones
print("\n--- Dataset1 ---")

# 1. airline_name -> MAYUSCULAS
df1['airline_name'] = df1['airline_name'].str.strip().str.upper()
print(f"airline_name normalizado a mayusculas. Valores unicos: {df1['airline_name'].nunique()}")

# 2. origin_airport -> MAYUSCULAS
df1['origin_airport'] = df1['origin_airport'].str.strip().str.upper()
print(f"origin_airport normalizado a mayusculas. Valores unicos: {df1['origin_airport'].nunique()}")

# 3. destination_airport -> MAYUSCULAS
df1['destination_airport'] = df1['destination_airport'].str.strip().str.upper()
print(f"destination_airport normalizado a mayusculas. Valores unicos: {df1['destination_airport'].nunique()}")

# 4. departure_datetime -> parsear a datetime
df1['departure_datetime'] = parsear_fechas(df1['departure_datetime'])
print(f"departure_datetime parseado. Nulos: {df1['departure_datetime'].isna().sum()}")

# 5. arrival_datetime -> parsear a datetime, nulos se quedan (vuelos cancelados)
df1['arrival_datetime'] = parsear_fechas(df1['arrival_datetime'])
print(f"arrival_datetime parseado. Nulos: {df1['arrival_datetime'].isna().sum()} (cancelados)")

# 6. duration_min, delay_min, seat -> nulos se dejan como NULL (cancelados)
df1['duration_min'] = pd.to_numeric(df1['duration_min'], errors='coerce')
df1['delay_min'] = pd.to_numeric(df1['delay_min'], errors='coerce')
print(f"duration_min nulos: {df1['duration_min'].isna().sum()} | delay_min nulos: {df1['delay_min'].isna().sum()} | seat nulos: {df1['seat'].isna().sum()}")

# DATASET 2 - Transformaciones
print("\n--- Dataset2 ---")

# passenger_gender -> normalizar a M, F, X
mapeo_genero = {
    'MASCULINO': 'M', 'FEMENINO': 'F', 'NOBINARIO': 'X',
}
df2['passenger_gender'] = df2['passenger_gender'].str.strip().str.upper().replace(mapeo_genero)
print(f"passenger_gender normalizado. Valores: {df2['passenger_gender'].value_counts().to_dict()}")

# passenger_age -> nulos se dejan como NULL
df2['passenger_age'] = pd.to_numeric(df2['passenger_age'], errors='coerce')
print(f"passenger_age nulos: {df2['passenger_age'].isna().sum()}")

# passenger_nationality -> nulos se dejan como NULL
print(f"passenger_nationality nulos: {df2['passenger_nationality'].isna().sum()}")

# booking_datetime -> parsear a datetime
df2['booking_datetime'] = parsear_fechas(df2['booking_datetime'])
print(f"booking_datetime parseado. Nulos: {df2['booking_datetime'].isna().sum()}")

# sales_channel -> nulos se dejan como NULL
print(f"sales_channel nulos: {df2['sales_channel'].isna().sum()}")

# ticket_price -> reemplazar coma por punto y convertir a float
df2['ticket_price'] = df2['ticket_price'].astype(str).str.replace(',', '.', regex=False)
df2['ticket_price'] = pd.to_numeric(df2['ticket_price'], errors='coerce')
print(f"ticket_price convertido a float. Nulos: {df2['ticket_price'].isna().sum()}")

 
# UNION DE DATAFRAMES
# Unimos por indice (1 a 1) ya que ambos tienen 10001 registros
df_completo = pd.concat([df1, df2], axis=1)
print(f"\nDataframe unido: {len(df_completo)} registros, {len(df_completo.columns)} columnas")


# FASE 3: CARGA A SQL SERVER
print("\n" + "=" * 60)
print("FASE 3: CARGA")
print("\n")

import pyodbc

# Conexion a SQL Server (Docker)
SERVER = 'localhost,1433'
DATABASE = 'practica1_ss2'
USER = 'sa'
PASSWORD = 'Pr4ctica1#SS2'
DRIVER = '{ODBC Driver 18 for SQL Server}'

def conectar_db():
    conn_str = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};TrustServerCertificate=yes'
    return pyodbc.connect(conn_str)

def crear_modelo(cursor):
    """Crea las tablas del modelo dimensional si no existen."""
    # Leer y ejecutar el script SQL
    with open('./modelo.sql', 'r') as f:
        sql = f.read()

    # Separar por GO y ejecutar cada bloque
    bloques = sql.split('GO')
    for bloque in bloques:
        bloque = bloque.strip()
        if bloque and 'CREATE DATABASE' not in bloque and 'USE ' not in bloque:
            try:
                cursor.execute(bloque)
            except pyodbc.ProgrammingError as e:
                # La tabla ya existe, se ignora
                if '2714' in str(e):
                    pass
                else:
                    print(f"  Advertencia: {e}")

def cargar_dimension(cursor, tabla, columnas, datos):
    """Inserta valores unicos en una tabla de dimension y retorna un dict de mapeo valor->id."""
    mapeo = {}
    for valor in datos:
        if pd.isna(valor):
            continue
        valor_str = str(valor).strip()
        if valor_str in mapeo:
            continue

        # Verificar si ya existe
        cols_where = ' AND '.join([f"{c} = ?" for c in columnas])
        cursor.execute(f"SELECT TOP 1 * FROM {tabla} WHERE {cols_where}", *([valor_str] * len(columnas)))
        fila = cursor.fetchone()
        if fila:
            mapeo[valor_str] = fila[0]
        else:
            cols_insert = ', '.join(columnas)
            placeholders = ', '.join(['?'] * len(columnas))
            cursor.execute(f"INSERT INTO {tabla} ({cols_insert}) VALUES ({placeholders})", *([valor_str] * len(columnas)))
            cursor.execute("SELECT @@IDENTITY")
            mapeo[valor_str] = int(cursor.fetchone()[0])
    return mapeo

def cargar_dimension_tiempo(cursor, fechas):
    """Inserta fechas unicas en dim_tiempo y retorna mapeo fecha->id."""
    mapeo = {}
    for fecha in fechas:
        if pd.isna(fecha):
            continue
        fecha_dt = pd.Timestamp(fecha)
        fecha_date = fecha_dt.date()
        fecha_str = str(fecha_date)

        if fecha_str in mapeo:
            continue

        cursor.execute("SELECT id_tiempo FROM dim_tiempo WHERE fecha = ?", fecha_date)
        fila = cursor.fetchone()
        if fila:
            mapeo[fecha_str] = fila[0]
        else:
            anio = fecha_dt.year
            mes = fecha_dt.month
            dia = fecha_dt.day
            trimestre = (mes - 1) // 3 + 1
            cursor.execute(
                "INSERT INTO dim_tiempo (fecha, anio, mes, dia, trimestre) VALUES (?, ?, ?, ?, ?)",
                fecha_date, anio, mes, dia, trimestre
            )
            cursor.execute("SELECT @@IDENTITY")
            mapeo[fecha_str] = int(cursor.fetchone()[0])
    return mapeo

def cargar_dimension_pasajero(cursor, df):
    """Inserta pasajeros unicos en dim_pasajero."""
    mapeo = {}
    for _, row in df.iterrows():
        pid = str(row['passenger_id']).strip()
        if pid in mapeo:
            continue

        cursor.execute("SELECT id_pasajero FROM dim_pasajero WHERE passenger_id = ?", pid)
        fila = cursor.fetchone()
        if fila:
            mapeo[pid] = fila[0]
        else:
            genero = row['passenger_gender'] if pd.notna(row['passenger_gender']) else None
            edad = int(row['passenger_age']) if pd.notna(row['passenger_age']) else None
            nacionalidad = row['passenger_nationality'] if pd.notna(row['passenger_nationality']) else None
            cursor.execute(
                "INSERT INTO dim_pasajero (passenger_id, genero, edad, nacionalidad) VALUES (?, ?, ?, ?)",
                pid, genero, edad, nacionalidad
            )
            cursor.execute("SELECT @@IDENTITY")
            mapeo[pid] = int(cursor.fetchone()[0])
    return mapeo

def cargar_datos():
    """Ejecuta la carga completa de dimensiones y hechos."""
    conn = conectar_db()
    cursor = conn.cursor()

    print("Creando modelo dimensional...")
    crear_modelo(cursor)
    conn.commit()

    # Cargar dimensiones simples
    print("Cargando dimensiones...")

    # Aerolinea: necesita codigo + nombre
    map_aerolinea = {}
    for _, row in df1[['airline_code', 'airline_name']].drop_duplicates().iterrows():
        code = str(row['airline_code']).strip()
        name = str(row['airline_name']).strip()
        key = code
        if key in map_aerolinea:
            continue
        cursor.execute("SELECT id_aerolinea FROM dim_aerolinea WHERE codigo = ?", code)
        fila = cursor.fetchone()
        if fila:
            map_aerolinea[key] = fila[0]
        else:
            cursor.execute("INSERT INTO dim_aerolinea (codigo, nombre) VALUES (?, ?)", code, name)
            cursor.execute("SELECT @@IDENTITY")
            map_aerolinea[key] = int(cursor.fetchone()[0])
    print(f"  dim_aerolinea: {len(map_aerolinea)} registros")

    # Aeropuertos (union de origen y destino)
    todos_aeropuertos = pd.concat([df1['origin_airport'], df1['destination_airport']]).dropna().unique()
    map_aeropuerto = cargar_dimension(cursor, 'dim_aeropuerto', ['codigo'], todos_aeropuertos)
    print(f"  dim_aeropuerto: {len(map_aeropuerto)} registros")

    # Estado
    map_estado = cargar_dimension(cursor, 'dim_estado_vuelo', ['estado'], df1['status'].dropna().unique())
    print(f"  dim_estado_vuelo: {len(map_estado)} registros")

    # Tipo avion
    map_avion = cargar_dimension(cursor, 'dim_tipo_avion', ['tipo'], df1['aircraft_type'].dropna().unique())
    print(f"  dim_tipo_avion: {len(map_avion)} registros")

    # Clase cabina
    map_clase = cargar_dimension(cursor, 'dim_clase_cabina', ['clase'], df1['cabin_class'].dropna().unique())
    print(f"  dim_clase_cabina: {len(map_clase)} registros")

    # Canal de venta
    map_canal = cargar_dimension(cursor, 'dim_canal_venta', ['canal'], df2['sales_channel'].dropna().unique())
    print(f"  dim_canal_venta: {len(map_canal)} registros")

    # Metodo pago
    map_metodo = cargar_dimension(cursor, 'dim_metodo_pago', ['metodo'], df2['payment_method'].dropna().unique())
    print(f"  dim_metodo_pago: {len(map_metodo)} registros")

    # Moneda
    map_moneda = cargar_dimension(cursor, 'dim_moneda', ['moneda'], df2['currency'].dropna().unique())
    print(f"  dim_moneda: {len(map_moneda)} registros")

    # Tiempo (fechas de salida, llegada y reserva)
    todas_fechas = pd.concat([
        df1['departure_datetime'].dropna(),
        df1['arrival_datetime'].dropna(),
        df2['booking_datetime'].dropna()
    ])
    map_tiempo = cargar_dimension_tiempo(cursor, todas_fechas)
    print(f"  dim_tiempo: {len(map_tiempo)} registros")

    # Pasajeros
    map_pasajero = cargar_dimension_pasajero(cursor, df2)
    print(f"  dim_pasajero: {len(map_pasajero)} registros")

    conn.commit()

    # Cargar tabla de hechos
    print("Cargando tabla de hechos...")

    # Funcion auxiliar: busca en mapeo solo si el valor no es nulo
    def buscar(mapeo, valor):
        if pd.isna(valor): return None
        return mapeo.get(str(valor).strip())

    def buscar_tiempo(valor):
        if pd.isna(valor): return None
        return map_tiempo.get(str(pd.Timestamp(valor).date()))

    total = 0
    for i in range(len(df_completo)):
        row = df_completo.iloc[i]

        cursor.execute("""
            INSERT INTO fact_vuelo (
                record_id, flight_number, seat,
                id_aerolinea, id_aeropuerto_origen, id_aeropuerto_destino,
                id_pasajero, id_tiempo_salida, id_tiempo_llegada, id_tiempo_reserva,
                id_canal, id_metodo, id_estado, id_tipo_avion, id_clase, id_moneda,
                duracion_min, retraso_min, precio_ticket, precio_usd,
                equipaje_total, equipaje_documentado
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            int(row['record_id']),
            str(row['flight_number']) if pd.notna(row['flight_number']) else None,
            str(row['seat']) if pd.notna(row['seat']) else None,
            buscar(map_aerolinea, row['airline_code']),
            buscar(map_aeropuerto, row['origin_airport']),
            buscar(map_aeropuerto, row['destination_airport']),
            buscar(map_pasajero, row['passenger_id']),
            buscar_tiempo(row['departure_datetime']),
            buscar_tiempo(row['arrival_datetime']),
            buscar_tiempo(row['booking_datetime']),
            buscar(map_canal, row['sales_channel']),
            buscar(map_metodo, row['payment_method']),
            buscar(map_estado, row['status']),
            buscar(map_avion, row['aircraft_type']),
            buscar(map_clase, row['cabin_class']),
            buscar(map_moneda, row['currency']),
            float(row['duration_min']) if pd.notna(row['duration_min']) else None,
            float(row['delay_min']) if pd.notna(row['delay_min']) else None,
            float(row['ticket_price']) if pd.notna(row['ticket_price']) else None,
            float(row['ticket_price_usd_est']) if pd.notna(row['ticket_price_usd_est']) else None,
            int(row['bags_total']) if pd.notna(row['bags_total']) else None,
            int(row['bags_checked']) if pd.notna(row['bags_checked']) else None
        )
        total += 1
        if total % 2000 == 0:
            print(f"  {total} registros insertados...")
            conn.commit()

    conn.commit()
    print(f"  Total: {total} registros insertados en fact_vuelo")

    cursor.close()
    conn.close()
    print("\nCarga completada.")

# Ejecutar carga
cargar_datos()