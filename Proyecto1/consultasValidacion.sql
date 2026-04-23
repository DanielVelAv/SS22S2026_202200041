USE SGFood_DW;
GO

-- 1. Conteo general de registros por tabla
SELECT 'Dim_Cliente' AS Tabla, COUNT(*) AS TotalRegistros FROM Dim_Cliente UNION ALL
SELECT 'Dim_Vendedor', COUNT(*) FROM Dim_Vendedor UNION ALL
SELECT 'Dim_Producto', COUNT(*) FROM Dim_Producto UNION ALL
SELECT 'Dim_Sucursal', COUNT(*) FROM Dim_Sucursal UNION ALL
SELECT 'Dim_Proveedor', COUNT(*) FROM Dim_Proveedor UNION ALL
SELECT 'Dim_Tiempo', COUNT(*) FROM Dim_Tiempo UNION ALL
SELECT 'Fact_Ventas', COUNT(*) FROM Fact_Ventas UNION ALL
SELECT 'Fact_Compras', COUNT(*) FROM Fact_Compras;

-- 2. Verificar que no hay ventas huérfanas
SELECT TOP 10 * FROM Fact_Ventas 
WHERE CodCliente IS NULL OR CodProducto IS NULL;