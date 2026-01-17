import sys
import os
# Configuración de paths para importar 'comun'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from pyspark.sql import functions as F

def limpiar_datos_plata():
    spark = obtener_sesion_spark("Limpieza_Bronze_a_Plata")
    
    # 1. Definir rutas
    path_bronce = "s3a://bronze/smart_inventory/ventas_procesadas"
    path_silver = "s3a://silver/smart_inventory/ventas_limpias"

    print(f">>> Leyendo datos desde Bronze: {path_bronce}")
    
    # 2. Leer Parquet de Bronze
    df_bronce = spark.read.parquet(path_bronce)

    # 3. Transformaciones de Limpieza (Ajustado a tus columnas reales)
    print(">>> Aplicando transformaciones de limpieza...")
    
    df_silver = df_bronce \
        .withColumn("fecha", F.to_date(F.col("fecha"))) \
        .withColumn("cantidad_vendida", F.col("cantidad_vendida").cast("int")) \
        .withColumn("precio_unitario", F.col("precio_unitario").cast("double")) \
        .withColumn("ingreso_total", F.col("ingreso_total").cast("double")) \
        .withColumn("stock_inicio_dia", F.col("stock_inicio_dia").cast("int")) \
        .withColumn("stock_fin_dia", F.col("stock_fin_dia").cast("int")) \
        .withColumn("quiebre_stock", F.col("quiebre_stock").cast("boolean"))

    # 4. Calidad de Datos (Data Quality)
    count_antes = df_silver.count()
    
    df_silver = df_silver.filter(
        (F.col("producto_id").isNotNull()) &
        (F.col("fecha").isNotNull()) &
        (F.col("cantidad_vendida") >= 0)
    ).dropDuplicates(["fecha", "producto_id", "tienda_id"])

    count_despues = df_silver.count()
    print(f">>> Registros procesados: {count_antes} | Registros limpios: {count_despues}")

    # 5. Escribir en Silver (Bucket MinIO)
    print(f">>> Escribiendo en Bucket Silver: {path_silver}")
    df_silver.write.mode("overwrite").parquet(path_silver)

    print(">>> Proceso de Limpieza (Plata) completado con éxito.")
    spark.stop()

if __name__ == "__main__":
    limpiar_datos_plata()