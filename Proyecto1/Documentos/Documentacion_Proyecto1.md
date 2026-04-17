# Proyecto 1: Sistema de Inteligencia de Negocios SG-Food
**Carnet:** 202200041

---

## Índice
1. [Introducción](#1-introducción)
   - 1.1. Objetivos del Proyecto
   - 1.2. Tecnologías Utilizadas
2. [Definición del Modelo de Datos (Data Warehouse)](#2-definición-del-modelo-de-datos)
   - 2.1. Arquitectura: Modelo Constelación (Galaxia)
   - 2.2. Granularidad de los Datos
   - 2.3. Diccionario de Datos Histórico
3. [Fases del Proceso ETL (Integration Services)](#3-fases-del-proceso-etl)
   - 3.1. Fase 0: Idempotencia y Preparación
   - 3.2. Fase 1: Extracción y Staging Area
   - 3.3. Fase 2: Transformación y Limpieza (Reglas de Negocio)
   - 3.4. Fase 3: Carga (Carga de Dimensiones y Hechos)
4. [Validaciones de Integridad y Calidad de Datos](#4-validaciones-de-integridad-y-calidad-de-datos)
5. [Implementación del Cubo OLAP (Analysis Services)](#5-implementación-del-cubo-olap-ssas)
   - 5.1. Configuración del DSV y Medidas
   - 5.2. Diseño de Jerarquías Analíticas
   - 5.3. Relaciones y Procesamiento (Deployment)
6. [Conclusiones y Lecciones Aprendidas](#6-conclusiones-y-lecciones-aprendidas)

---

## 1. Introducción
El presente documento detalla el diseño, desarrollo e implementación de una solución integral de Inteligencia de Negocios (BI) para la empresa SG-Food. Ante la necesidad de consolidar información histórica de compras y ventas que presentaba serios problemas de calidad y formato (sabotaje de datos), se construyó un Data Warehouse centralizado que permite a los tomadores de decisiones cruzar métricas operativas de forma rápida y confiable a través un Cubo OLAP.

### 1.1. Objetivos del Proyecto
*   **General:** Construir una solución End-to-End de Business Intelligence que consolide los procesos de compras y ventas de SG-Food en un solo entorno analítico multidimensional.
*   **Específicos:**
    *   Diseñar un modelo capaz de soportar comparativas entre múltiples procesos transaccionales sin duplicar dimensiones maestras.
    *   Desarrollar un flujo ETL automatizado que depure anomalías estructurales severas en los archivos planos de origen (fechas malformadas, negativos, espacios en blanco).
    *   Desplegar un Cubo OLAP interactivo mediante jerarquías (Drill-down y Roll-up) para un fácil consumo del negocio.

### 1.2. Tecnologías Utilizadas
*   **Motor Relacional:** SQL Server 2025 Developer Edition (Almacenamiento del Data Warehouse y esquema de Staging).
*   **ETL:** Visual Studio 2022 con extensión SQL Server Integration Services (SSIS).
*   **OLAP:** SQL Server Analysis Services 2022 (SSAS) en modo Multidimensional.
*   **Procesamiento:** Transact-SQL (T-SQL) para limpiezas avanzadas y validación de llaves.

---

## 2. Definición del Modelo de Datos (Data Warehouse)

### 2.1. Arquitectura: Modelo Constelación (Galaxia)
Para satisfacer los requerimientos analíticos exigidos por SG-Food, se descartó el tradicional esquema de Estrella (Star Schema) en favor de un **Modelo de Constelación de Hechos (Fact Constellation)**. 
El negocio exigía analizar dos procesos totalmente distintos (Ventas vs Compras), por lo que esta arquitectura despliega dos tablas de hechos independientes (`Fact_Ventas` y `Fact_Compras`) orbitando alrededor de "Dimensiones Conformadas" compartidas, tales como `Dim_Tiempo`, `Dim_Producto` y `Dim_Sucursal`. Esto permite un cruce de indicadores limpio sin sobrecargar la Base de Datos.

### 2.2. Granularidad de los Datos
El grano definido para ambas tablas de hechos transaccionales es la **línea de detalle por transacción diaria**. Cada registro almacena la agregación más profunda posible (compra/venta de un producto específico, en una sucursal y día específico), lo que asegura la máxima flexibilidad analítica al escalar en el cubo OLAP.

### 2.3. Diccionario de Datos Histórico
*   **Tablas de Hechos (Fact Tables):**
    *   `Fact_Ventas`: Almacena métricas comerciales de salida. Medidas cuantitativas: `Unidades` y `PrecioUnitario`.
    *   `Fact_Compras`: Almacena métricas de reabastecimiento. Medidas cuantitativas: `Unidades` y `CostoUnitario`.
*   **Dimensiones Compartidas (Conformed Dimensions):**
    *   `Dim_Tiempo`: Dimensión generada transaccionalmente a partir de fechas válidas (Día, Mes, Año).
    *   `Dim_Producto`: Atributos descriptivos normalizados (Nombre, Marca, Categoría).
    *   `Dim_Sucursal`: Permite el análisis geográfico (Región, Departamento, Sucursal).
*   **Dimensiones Exclusivas:**
    *   `Dim_Cliente` y `Dim_Vendedor`: Conectadas únicamente al proceso de Ventas.
    *   `Dim_Proveedor`: Conectada únicamente al proceso de Compras.

*(Nota: Insertar CAPTURA de PANTALLA del diagrama de la Base de Datos o Data Source View de SSAS).*

---

## 3. Fases del Proceso ETL (Integration Services)

El proceso de Extracción, Transformación y Carga (ETL) debió enfrentar archivos `.csv` gravemente corrompidos de forma intencional; para ello se utilizó una arquitectura robusta de limpieza en dos capas.

### 3.1. Fase 0: Idempotencia y Preparación
El diseño del *Control Flow* asegura que el paquete no duplique información si el usuario presiona "Start" múltiples veces. El primer paso del flujo es un contenedor `Execute SQL Task` que ejecuta un truncado/eliminación (`DELETE`) respetando la Integridad Referencial: vaciando primero las tablas de hechos (`Fact_Ventas` y `Fact_Compras`) y posteriormente los catálogos dimensionales (`Dim_Cliente`, etc.).

### 3.2. Fase 1: Extracción y Staging Area
Los datos planos se leyeron mediante *Flat File Managers*. Dada la severidad del "ruido" en los datos que los componentes nativos de SSIS (como el "Sort" o "Derived Column") no pueden procesar eficientemente sin colapsar la memoria caché, los datos crudos se cargaron en un **Área de Staging** (tablas temporales `tmp_Ventas` y `tmp_Compra`), delegando la limpieza intensiva al motor T-SQL.

### 3.3. Fase 2: Transformación y Limpieza (Reglas de Negocio)
Dentro del Data Warehouse (en el nivel de Staging), se programaron reglas estrictas de limpieza y transformación:
1.  **Infiltración de Letras en Fechas (Z3/08/2018):** Se descubrieron fechas corrompidas con la letra 'Z'. Se normalizaron forzando un reintegro matemático con la función envolvente: `TRY_CONVERT(DATE, REPLACE(UPPER(TRIM(Fecha)), 'Z', '2'), 103)`. Esta validación garantizó que únicamente Fechas con formato británico (103) pasaran a `Dim_Tiempo`.
2.  **Valores Unitarios Negativos y Faltantes:** Se filtraron montos irreales utilizando T-SQL condicional explícito en los *INSERT* de migración: `(Unidades > 0 AND PrecioUnitario > 0)` y se descartó la asimilación de proveedores sin identificador: `(CodProveedor IS NOT NULL AND CodProveedor <> 'Sin código')`.
3.  **Sensibilidad a Mayúsculas y Blancos:** Todo texto dimensional fue pasado por la combinación `UPPER(TRIM(Campo))` para evitar que "Bebidas " y "bebidas" generaran duplicados de *Primary Key* en SSIS.

### 3.4. Fase 3: Carga (Carga de Dimensiones y Hechos)
Posterior a la limpieza, los registros catalogados se ingresaron a sus respectivas Dimensiones usando la función `NOT IN (SELECT...)` para almacenar únicamente IDs faltantes. Por último, las métricas limpias se vertieron sobre las tablas Fact_Ventas y Fact_Compras.

---

## 4. Validaciones de Integridad y Calidad de Datos
Para demostrar la eficacia del sistema, se codificaron consultas de validación en el archivo `consultas_validacion.sql`.
*   Se realizó un barrido matemático (`SELECT COUNT(*)`) sobre las tablas de dimensiones descartando una migración nula.
*   Se corrió la consulta `SELECT TOP 10 * FROM Fact_Ventas WHERE CodCliente IS NULL OR CodProducto IS NULL;` para probar empíricamente que no existe ninguna Venta "huérfana" o desconectada de un maestro en la Constelación.

---

## 5. Implementación del Cubo OLAP (SSAS)

Cumplido el ciclo ETL, la Base de Datos se conectó a **SQL Server Analysis Services (SSAS)**.

### 5.1. Configuración del DSV y Medidas
Se construyó un *Data Source View (DSV)* conector de 8 tablas. Dentro del Cubo, el sistema reconoció fluidamente la arquitectura dividiéndose en dos *Measure Groups* (Grupos de Medida): Compras y Ventas.

### 5.2. Diseño de Jerarquías Analíticas
Para potenciar la navegabilidad del negocio mediante operaciones Drill-down/Drill-up, se definieron manualmente las siguientes jerarquías dentro de las dimensiones:
*   **Geografía Comercial (Dim Sucursal):** `Región -> Departamento -> Sucursal`.
*   **Cronología de Negocio (Dim Tiempo):** `Año -> Mes -> Día`.

### 5.3. Relaciones y Procesamiento (Deployment)
*   **Resolución de Bugs de Compatibilidad:** Al encontrarse trabajando bajo *Developer Edition 2025*, el cliente de Visual Studio rechaza la cadena "StandardDeveloper64". Esto se solucionó implementando un motor paralelo SSAS versión 2022 y ajustando el *Target Server* del proyecto.
*   **Seguridad OLAP:** La conectividad OLE DB entre SSAS y el SQL Server se autorizó configurando el *Impersonation Mode* del Data Source bajo credenciales de administrador para permitir la carga masiva ("Process Full").

*(Nota: Insertar CAPTURA de PANTALLA del Browser del cubo en Excel o Visual Studio).*

---

## 6. Conclusiones y Lecciones Aprendidas
1.  **Arquitectura Constelación Indispensable:** El requerimiento puntual de procesar dos "Facts" paralelos (suministros vs distribución) hizo imposible usar un modelo de Estrella. La Galaxia permitió reutilizar dimensiones (como *Tiempo* y *Producto*) maximizando el rendimiento analítico sin duplicar espacios en el Data Warehouse.
2.  **Staging T-SQL como Salvavidas de Datos Severos:** SSIS es poderoso para enrutar datos masivos, sin embargo, limpiezas de alto nivel como la evasión de Foreign Keys por strings anómalos o reemplazos de letras en parámetros tipo *Date* bloquean los Buffers nativos. Implementar una capa intermedia (Status: Staging) le permitió a SQL Server manejar los `REPLACE` perimetrales en milisegundos.
3.  **Dependencia Total de los Permisos (Impersonation Mode):** La estructura del cubo puede estar perfectamente modelada, pero sin las credenciales del sistema nativo inyectadas al entorno SSAS, resulta imposible ejecutar el "Deployment" final hacia el explorador. Entender el flujo de comunicación OLAP vs. Motor Relacional fue el reto principal que se superó en la entrega.