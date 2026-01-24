import sys
import os
import boto3
from botocore.exceptions import ClientError

# Configuración de paths para importar 'comun'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from pyspark.sql.functions import col, to_date, when

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

def limpiar_datos_plata():
    # 0. Garantizar bucket 'silver'
    garantizar_bucket("silver")

    spark = obtener_sesion_spark("Limpieza_Bronce_a_Plata")
    
    path_bronce = "s3a://bronze/smart_inventory/ventas_procesadas"
    path_silver = "s3a://silver/smart_inventory/ventas_limpias"

    print(f">>> Leyendo de Bronce: {path_bronce}")
    df = spark.read.parquet(path_bronce)

    # Lógica de Limpieza
    print(">>> Aplicando reglas de limpieza...")
    
    # 1. Convertir fecha string a date real (si no lo es)
    # 2. Rellenar nulos en categoricos
    # 3. Filtrar ventas con cantidad negativa (errores de sistema)
    
    df_clean = df.withColumn("fecha", to_date(col("fecha"))) \
                 .withColumn("categoria", when(col("categoria").isNull(), "Sin Categoria").otherwise(col("categoria"))) \
                 .filter(col("cantidad_vendida") >= 0)

    print(f">>> Registros antes: {df.count()} | Registros despues: {df_clean.count()}")

    print(f">>> Escribiendo en Plata: {path_silver}")
    df_clean.write.mode("overwrite").parquet(path_silver)
    
    print("✅ Limpieza completada.")

if __name__ == "__main__":
    limpiar_datos_plata()