import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from comun.sesion_spark import obtener_sesion_spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def generar_features_oro():
    spark = obtener_sesion_spark("Features_Silver_a_Oro")
    path_silver = "s3a://silver/smart_inventory/ventas_limpias"
    path_oro_features = "s3a://gold/smart_inventory/dataset_features"

    df_silver = spark.read.parquet(path_silver)
    window_spec = Window.partitionBy("tienda_id", "producto_id").orderBy("fecha")

    df_features = (
        df_silver
        .withColumn("dia_semana", F.dayofweek(F.col("fecha")))
        .withColumn("es_fin_semana", F.when(F.col("dia_semana").isin(1, 7), 1).otherwise(0))
        .withColumn("mes", F.month(F.col("fecha")))
        .withColumn("ventas_lag_1", F.lag("cantidad_vendida", 1).over(window_spec))
        .withColumn("ventas_lag_7", F.lag("cantidad_vendida", 7).over(window_spec))
        .withColumn("media_movil_7d", F.avg("cantidad_vendida").over(window_spec.rowsBetween(-7, -1)))
        .withColumn("target_ventas_proximo_dia", F.lead("cantidad_vendida", 1).over(window_spec))
    )

    # IMPORTANTE: Solo borrar nulos si existen suficientes datos para los lags
    df_final = df_features.filter(F.col("ventas_lag_1").isNotNull())
    
    print(f">>> Columnas generadas: {df_final.columns}")
    df_final.write.mode("overwrite").parquet(path_oro_features)
    spark.stop()

if __name__ == "__main__":
    generar_features_oro()