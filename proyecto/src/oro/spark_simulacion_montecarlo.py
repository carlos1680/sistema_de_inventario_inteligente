import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from servicios.save_db import guardar_en_mariadb
# CORRECCIÓN: Importar PipelineModel en lugar de RandomForestRegressionModel
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, lit, rand

def main():
    spark = obtener_sesion_spark("Simulacion_MonteCarlo")
    
    path_gold_features = "s3a://gold/smart_inventory/dataset_features"
    path_modelo = "s3a://models/smart_inventory/rf_demanda_v1"

    print(f">>> Leyendo datos base: {path_gold_features}")
    df = spark.read.parquet(path_gold_features)
    
    print(f">>> Cargando modelo desde: {path_modelo}")
    try:
        # CORRECCIÓN: Cargar como PipelineModel
        modelo = PipelineModel.load(path_modelo)
        print("✅ Modelo (Pipeline) cargado correctamente.")
    except Exception as e:
        print(f"❌ Error cargando el modelo en {path_modelo}: {e}")
        raise e

    print(">>> Iniciando Simulación Monte Carlo...")
    
    # Predecir
    df_pred = modelo.transform(df)
    
    # Generar escenarios (Monte Carlo simple)
    df_simulacion = df_pred.withColumn("demanda_base", col("prediction")) \
                           .withColumn("demanda_simulada_p50", col("demanda_base")) \
                           .withColumn("demanda_simulada_p90", col("demanda_base") * 1.25) \
                           .withColumn("demanda_simulada_p10", col("demanda_base") * 0.75)

    # Seleccionar columnas finales
    df_final = df_simulacion.select(
        col("fecha"),
        col("tienda_id"),
        col("producto_id"),
        col("demanda_simulada_p50").cast("int").alias("demanda_esperada"),
        col("demanda_simulada_p90").cast("int").alias("demanda_optimista"),
        col("demanda_simulada_p10").cast("int").alias("demanda_pesimista")
    )

    print(">>> Resultados de la simulación:")
    df_final.show(5)

    # Guardar en MariaDB
    tabla_destino = "inventario_predicciones"
    guardar_en_mariadb(df_final, tabla_destino)
    
    print("✅ Proceso finalizado exitosamente.")

if __name__ == "__main__":
    main()