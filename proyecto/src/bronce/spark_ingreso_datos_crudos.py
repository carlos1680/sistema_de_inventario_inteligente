import sys
import os
# Configuración de paths para importar 'comun'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from pyspark.sql.functions import current_timestamp, lit

def ingesta_landing_a_bronce():
    spark = obtener_sesion_spark("Ingesta_Landing_a_Bronce")
    
    # 1. Definir rutas
    # Path local (visto desde el contenedor de Spark)
    path_landing = "/opt/minio/shareddata/smart_inventory/landing/ventas_ejemplo.csv"
    
    # Path S3 (visto por la red de MinIO)
    path_bronce = "s3a://bronze/smart_inventory/ventas_procesadas"

    print(f">>> Leyendo desde Landing: {path_landing}")
    
    # 2. Leer CSV
    df_raw = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(path_landing)

    # 3. Agregar metadata (Auditoría)
    # Es buena práctica en Bronce saber cuándo se ingesto el dato y de qué archivo vino
    df_bronce = df_raw.withColumn("fecha_ingesta", current_timestamp()) \
                      .withColumn("archivo_origen", lit("ventas_ejemplo.csv"))

    print(">>> Esquema detectado:")
    df_bronce.printSchema()

    # 4. Escribir en Bronce (Bucket MinIO) como Parquet
    print(f">>> Escribiendo en Bucket Bronze: {path_bronce}")
    df_bronce.write.mode("overwrite").parquet(path_bronce)

    print(">>> Ingesta completada con éxito.")
    spark.stop()

if __name__ == "__main__":
    ingesta_landing_a_bronce()