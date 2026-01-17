CREATE DATABASE IF NOT EXISTS inventario_db;
USE inventario_db;

-- Tabla para el forecast de demanda
CREATE TABLE IF NOT EXISTS reporte_forecast (
    fecha DATE,
    producto_id VARCHAR(50),
    tienda_id VARCHAR(50),
    demanda_predicha FLOAT,
    error_estimado FLOAT,
    fecha_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para los resultados de Monte Carlo
CREATE TABLE IF NOT EXISTS reporte_simulacion_stock (
    producto_id VARCHAR(50),
    tienda_id VARCHAR(50),
    probabilidad_quiebre FLOAT,
    stock_sugerido INT,
    nivel_servicio_objetivo FLOAT,
    fecha_proceso TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);