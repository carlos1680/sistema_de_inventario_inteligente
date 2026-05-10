import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from comun.minio_utils import garantizar_bucket
from pyspark.sql.functions import col, lag, avg, dayofweek, month
from pyspark.sql.window import Window

def generar_features(df):
    """Genera features de lag, media móvil y temporales sobre un DataFrame Silver."""
    w = Window.partitionBy("tienda_id", "producto_id").orderBy("fecha")
    return (
        df.withColumn("ventas_lag_1",      lag("cantidad_vendida", 1).over(w))
          .withColumn("ventas_lag_7",      lag("cantidad_vendida", 7).over(w))
          .withColumn("promedio_movil_7",  avg("cantidad_vendida").over(w.rowsBetween(-6, 0)))
          .withColumn("dia_semana",        dayofweek(col("fecha")))
          .withColumn("mes",               month(col("fecha")))
          .na.drop()
    )


def generar_features_oro():
    garantizar_bucket("gold")

    spark = obtener_sesion_spark("Ingenieria_Caracteristicas_Plata_a_Oro")

    path_silver       = "s3a://silver/smart_inventory/ventas_limpias"
    path_gold_features = "s3a://gold/smart_inventory/dataset_features"

    print(f">>> Leyendo de Plata: {path_silver}")
    df = spark.read.parquet(path_silver)

    df_features = generar_features(df)

    print(f">>> Escribiendo dataset de entrenamiento en Oro: {path_gold_features}")
    df_features.write.mode("overwrite").parquet(path_gold_features)

    print("✅ Ingeniería de características completada.")

if __name__ == "__main__":
    generar_features_oro()