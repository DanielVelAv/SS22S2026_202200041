
-- Crear la base de datos (Si no existe)
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'SGFood_DW')
BEGIN
    CREATE DATABASE SGFood_DW;
END
GO

USE SGFood_DW;
GO

-- ==============================================================================
-- 1. CREACIÓN DE TABLAS DE DIMENSIONES (Catálogos)
-- ==============================================================================

-- Dimensión Tiempo
CREATE TABLE Dim_Tiempo (
    Fecha DATE PRIMARY KEY,
    Dia INT,
    Mes INT,
    Anio INT
);

-- Dimensión Cliente
CREATE TABLE Dim_Cliente (
    CodCliente VARCHAR(50) PRIMARY KEY,
    NombreCliente VARCHAR(150),
    TipoCliente VARCHAR(50)
);

-- Dimensión Vendedor
CREATE TABLE Dim_Vendedor (
    CodVendedor VARCHAR(50) PRIMARY KEY,
    NombreVendedor VARCHAR(150)
);

-- Dimensión Producto
CREATE TABLE Dim_Producto (
    CodProducto VARCHAR(50) PRIMARY KEY,
    NombreProducto VARCHAR(150),
    MarcaProducto VARCHAR(100),
    Categoria VARCHAR(100)
);

-- Dimensión Sucursal
CREATE TABLE Dim_Sucursal (
    CodSucursal VARCHAR(50) PRIMARY KEY,
    NombreSucursal VARCHAR(150),
    Region VARCHAR(100),
    Departamento VARCHAR(100)
);

-- Dimensión Proveedor
CREATE TABLE Dim_Proveedor (
    CodProveedor VARCHAR(50) PRIMARY KEY,
    NombreProveedor VARCHAR(150)
);

-- 2. CREACIÓN DE TABLAS DE HECHOS (Transacciones)


-- Tabla de Hechos: Ventas
CREATE TABLE Fact_Ventas (
    IdVenta INT IDENTITY(1,1) PRIMARY KEY,
    Fecha DATE,
    CodCliente VARCHAR(50),
    CodVendedor VARCHAR(50),
    CodProducto VARCHAR(50),
    CodSucursal VARCHAR(50),
    Unidades INT,
    PrecioUnitario DECIMAL(18,2),
    -- Llaves foráneas
    FOREIGN KEY (Fecha) REFERENCES Dim_Tiempo(Fecha),
    FOREIGN KEY (CodCliente) REFERENCES Dim_Cliente(CodCliente),
    FOREIGN KEY (CodVendedor) REFERENCES Dim_Vendedor(CodVendedor),
    FOREIGN KEY (CodProducto) REFERENCES Dim_Producto(CodProducto),
    FOREIGN KEY (CodSucursal) REFERENCES Dim_Sucursal(CodSucursal)
);

-- Tabla de Hechos: Compras
CREATE TABLE Fact_Compras (
    IdCompra INT IDENTITY(1,1) PRIMARY KEY,
    Fecha DATE,
    CodProveedor VARCHAR(50),
    CodProducto VARCHAR(50),
    CodSucursal VARCHAR(50),
    Unidades INT,
    CostoUnitario DECIMAL(18,2),
    -- Llaves foráneas
    FOREIGN KEY (Fecha) REFERENCES Dim_Tiempo(Fecha),
    FOREIGN KEY (CodProveedor) REFERENCES Dim_Proveedor(CodProveedor),
    FOREIGN KEY (CodProducto) REFERENCES Dim_Producto(CodProducto),
    FOREIGN KEY (CodSucursal) REFERENCES Dim_Sucursal(CodSucursal)
);
