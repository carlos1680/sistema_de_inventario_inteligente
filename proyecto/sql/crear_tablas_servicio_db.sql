USE bigdata_db;

-- Resultados del modelo de forecast de demanda
CREATE TABLE IF NOT EXISTS reporte_forecast (
    fecha            DATE,
    producto_id      VARCHAR(50),
    tienda_id        VARCHAR(50),
    demanda_predicha DECIMAL(10,4),
    fecha_proceso    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_producto_fecha (producto_id, fecha)
);

-- Resultados de la simulación Monte Carlo de stock
CREATE TABLE IF NOT EXISTS reporte_simulacion_stock (
    fecha                   DATE,
    producto_id             VARCHAR(50),
    tienda_id               VARCHAR(50),
    probabilidad_quiebre    DECIMAL(5,4),
    demanda_p50             DECIMAL(10,4),
    demanda_p95_critica     DECIMAL(10,4),
    stock_promedio_esperado DECIMAL(10,4),
    fecha_proceso           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_producto_fecha (producto_id, fecha)
);
