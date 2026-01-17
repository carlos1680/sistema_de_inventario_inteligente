# src/oro/spark_simulacion_montecarlo.py
import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Importación de sesión común respetando tu estructura de carpetas
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from comun.sesion_spark import obtener_sesion_spark

# Importamos los componentes de ML necesarios
from pyspark.ml.regression import LinearRegressionModel 
from pyspark.ml.feature import VectorAssembler


def exportar_a_shared(df):
    """
    Exporta los resultados de la simulación a un CSV "humano" en /opt/shared.
    - Directorio: /opt/shared/resultados_montecarlo_csv
    - Formato: CSV con header, coalesce(1) para un único archivo de datos.
    """
    output_dir = "/opt/shared/resultados_montecarlo_csv"
    output_path = f"file://{output_dir}"

    print(f">>> Exportando resultados a CSV en: {output_dir}")

    (
        df.orderBy("producto_id", "fecha")
          .coalesce(1)
          .write
          .mode("overwrite")
          .option("header", True)
          .csv(output_path)
    )

    print("✅ Exportación a /opt/shared completada.")


def main():
    # Iniciar sesión con tu configuración de MinIO
    spark = obtener_sesion_spark("MonteCarlo_Inventario_Oro")
    
    # Configuración de la simulación
    NUM_SIMULACIONES = 100
    VOLATILIDAD_PCT = 0.20 
    SEED = 42

    # 2. Paths verificados según tu captura de MinIO y entrenamiento
    path_modelo = "s3a://gold/smart_inventory/modelos/modelo_demanda_lr"
    path_features = "s3a://gold/smart_inventory/dataset_features"
    path_salida = "s3a://gold/smart_inventory/resultados_montecarlo"

    print(f">>> Cargando modelo desde: {path_modelo}")
    modelo = LinearRegressionModel.load(path_modelo)

    # 3. Cargar features (Capa Gold/Oro en MinIO)
    print(f">>> Cargando features desde: {path_features}")
    df_features = spark.read.parquet(path_features)

    # 4. Preparar datos para el modelo (VectorAssembler)
    # Usamos exactamente las mismas columnas que en tu script de entrenamiento
    feature_cols = ["dia_semana", "es_fin_semana", "mes", "ventas_lag_1", "media_movil_7d"]
    
    # Filtramos nulos para evitar errores en el transform
    condicion_not_null = " AND ".join([f"{c} IS NOT NULL" for c in feature_cols])
    df_limpio = df_features.filter(condicion_not_null)

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    df_preparado = assembler.transform(df_limpio)

    # 5. Generar predicción base (Demanda Esperada)
    # Corregido: usamos 'producto_id' en lugar de 'product_id'
    df_predicciones = modelo.transform(df_preparado) \
        .select("producto_id", "fecha", F.col("prediction").alias("demanda_esperada"))

    print(f">>> Ejecutando {NUM_SIMULACIONES} simulaciones Monte Carlo...")

    # 6. Simulación Monte Carlo nativa
    # Replicamos filas para los escenarios
    df_simulacion = df_predicciones.withColumn(
        "simulacion_id", 
        F.explode(F.array([F.lit(i) for i in range(NUM_SIMULACIONES)]))
    )

    # Aplicamos ruido normal alrededor de la predicción
    df_montecarlo = df_simulacion.withColumn(
        "ruido", F.randn(SEED) * F.lit(VOLATILIDAD_PCT)
    ).withColumn(
        "demanda_simulada",
        F.when(
            (F.col("demanda_esperada") * (1 + F.col("ruido"))) < 0, 0
        ).otherwise(F.col("demanda_esperada") * (1 + F.col("ruido")))
    )

    # 7. Simulación de Stock y Riesgo de Quiebre
    # Como no hay tabla de stock, simulamos un stock inicial de 500 unidades
    print(">>> Calculando riesgo de quiebre con stock simulado (500 unidades)...")
    
    window_spec = Window.partitionBy("producto_id", "simulacion_id").orderBy("fecha")
    
    df_analisis = df_montecarlo \
        .withColumn("stock_inicial", F.lit(500)) \
        .withColumn("demanda_acumulada", F.sum("demanda_simulada").over(window_spec)) \
        .withColumn("stock_proyectado", F.col("stock_inicial") - F.col("demanda_acumulada")) \
        .withColumn("quiebre_stock", F.when(F.col("stock_proyectado") < 0, 1).otherwise(0))

    # 8. Agregación final de resultados
    df_resultado_oro = df_analisis.groupBy("producto_id", "fecha") \
        .agg(
            F.mean("quiebre_stock").alias("probabilidad_quiebre"),
            F.expr("percentile_approx(demanda_simulada, 0.5)").alias("demanda_p50"),
            F.expr("percentile_approx(demanda_simulada, 0.95)").alias("demanda_p95_critica"),
            F.avg("stock_proyectado").alias("stock_promedio_esperado")
        )

    # 9. Guardar resultados en MinIO (Capa Gold)
    print(f">>> Guardando resultados finales en: {path_salida}")
    df_resultado_oro.write.mode("overwrite").parquet(path_salida)
    print("✅ Resultados guardados en MinIO.")

    # 10. Exportar también a /opt/shared como CSV humano
    exportar_a_shared(df_resultado_oro)

    print("✅ Pipeline de Simulación Monte Carlo finalizado exitosamente.")
    spark.stop()


if __name__ == "__main__":
    main()