import sys
import os
import boto3
from botocore.exceptions import ClientError

# Configuración de paths para importar 'comun'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from pyspark.sql.functions import current_timestamp, lit

def garantizar_bucket(bucket_name):
    """
    Verifica si el bucket existe en MinIO y si no, lo crea.
    Usa las mismas credenciales del entorno.
    """
    print(f"🔍 Verificando existencia del bucket: '{bucket_name}'...")
    
    # Credenciales desde variables de entorno (las mismas que usa Spark)
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "admin123")

    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ El bucket '{bucket_name}' ya existe.")
    except ClientError:
        # Si falla el head_bucket, asumimos que no existe y lo creamos
        print(f"⚠️ El bucket '{bucket_name}' no existe. Creándolo...")
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"🎉 Bucket '{bucket_name}' creado exitosamente.")
        except Exception as e:
            print(f"❌ Error fatal creando el bucket: {e}")
            raise e

def ingesta_landing_a_bronce():
    # 0. Paso Previo: Garantizar que el bucket 'bronze' exista
    # Extraemos 'bronze' de la ruta 's3a://bronze/...'
    garantizar_bucket("bronze")

    spark = obtener_sesion_spark("Ingesta_Landing_a_Bronce")
    
    # 1. Definir rutas
    # Ruta física donde publish.sh dejó el archivo
    path_landing = "/opt/minio/shareddata/landing/smart_inventory/ventas_ejemplo.csv"
    
    # Path S3 donde Spark escribirá
    path_bronce = "s3a://bronze/smart_inventory/ventas_procesadas"

    print(f">>> Leyendo desde Landing: {path_landing}")
    
    # 2. Leer CSV
    try:
        df_raw = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(path_landing)
    except Exception as e:
        print(f"❌ Error leyendo el archivo en {path_landing}. Verifica que publish.sh haya corrido.")
        raise e

    # 3. Agregar metadata (Auditoría)
    df_bronce = df_raw.withColumn("fecha_ingesta", current_timestamp()) \
                      .withColumn("archivo_origen", lit("ventas_ejemplo.csv"))

    print(">>> Esquema detectado:")
    df_bronce.printSchema()

    # 4. Escribir en Bronce (Parquet en MinIO)
    print(f">>> Escribiendo en Bronce: {path_bronce}")
    df_bronce.write.mode("overwrite").parquet(path_bronce)
    
    print("✅ Ingesta completada exitosamente.")

if __name__ == "__main__":
    ingesta_landing_a_bronce()