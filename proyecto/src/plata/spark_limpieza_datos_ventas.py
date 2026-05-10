import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from comun.minio_utils import garantizar_bucket
from pyspark.sql.functions import col, to_date, when

def limpiar_ventas(df):
    """Aplica las reglas de limpieza Silver sobre un DataFrame de ventas."""
    return (
        df.withColumn("fecha", to_date(col("fecha")))
          .withColumn("categoria", when(col("categoria").isNull(), "Sin Categoria").otherwise(col("categoria")))
          .filter(col("cantidad_vendida") >= 0)
    )


def limpiar_datos_plata():
    garantizar_bucket("silver")

    spark = obtener_sesion_spark("Limpieza_Bronce_a_Plata")

    path_bronce = "s3a://bronze/smart_inventory/ventas_procesadas"
    path_silver = "s3a://silver/smart_inventory/ventas_limpias"

    print(f">>> Leyendo de Bronce: {path_bronce}")
    df = spark.read.parquet(path_bronce)

    df_clean = limpiar_ventas(df)
    print(f">>> Registros antes: {df.count()} | Registros despues: {df_clean.count()}")

    print(f">>> Escribiendo en Plata: {path_silver}")
    df_clean.write.mode("overwrite").parquet(path_silver)

    print("✅ Limpieza completada.")

if __name__ == "__main__":
    limpiar_datos_plata()