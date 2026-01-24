import sys
import os
import boto3
from botocore.exceptions import ClientError

# Configuración de paths para importar 'comun'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from pyspark.sql.functions import col, lag, avg, dayofweek, month
from pyspark.sql.window import Window

def garantizar_bucket(bucket_name):
    """Verifica si el bucket existe en MinIO y si no, lo crea."""
    print(f"🔍 Verificando existencia del bucket: '{bucket_name}'...")
    
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "admin123")

    s3_client = boto3.client('s3', endpoint_url=endpoint, aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ El bucket '{bucket_name}' ya existe.")
    except ClientError:
        print(f"⚠️ El bucket '{bucket_name}' no existe. Creándolo...")
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"🎉 Bucket '{bucket_name}' creado exitosamente.")
        except Exception as e:
            print(f"❌ Error fatal creando el bucket: {e}")
            raise e

def generar_features_oro():
    # 0. Garantizar bucket 'gold'
    garantizar_bucket("gold")

    spark = obtener_sesion_spark("Ingenieria_Caracteristicas_Plata_a_Oro")
    
    path_silver = "s3a://silver/smart_inventory/ventas_limpias"
    path_gold_features = "s3a://gold/smart_inventory/dataset_features"

    print(f">>> Leyendo de Plata: {path_silver}")
    df = spark.read.parquet(path_silver)

    print(">>> Generando Features (Lags y Medias Móviles)...")
    
    # Definir ventana por producto/tienda ordenada por fecha
    w = Window.partitionBy("tienda_id", "producto_id").orderBy("fecha")

    # Feature Engineering simple:
    # 1. Ventas del dia anterior (lag_1)
    # 2. Ventas de hace 7 dias (lag_7)
    # 3. Promedio movil 7 dias
    # 4. Dia de la semana
    
    df_features = df.withColumn("ventas_lag_1", lag("cantidad_vendida", 1).over(w)) \
                    .withColumn("ventas_lag_7", lag("cantidad_vendida", 7).over(w)) \
                    .withColumn("promedio_movil_7", avg("cantidad_vendida").over(w.rowsBetween(-6, 0))) \
                    .withColumn("dia_semana", dayofweek(col("fecha"))) \
                    .withColumn("mes", month(col("fecha"))) \
                    .na.drop() # Eliminar filas donde no se pudieron calcular lags (primeros dias)

    print(f">>> Escribiendo dataset de entrenamiento en Oro: {path_gold_features}")
    df_features.write.mode("overwrite").parquet(path_gold_features)
    
    print("✅ Ingeniería de características completada.")

if __name__ == "__main__":
    generar_features_oro()