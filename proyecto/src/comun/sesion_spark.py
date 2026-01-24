from pyspark.sql import SparkSession
import os

def obtener_sesion_spark(app_name="InventarioInteligente"):
    """
    Crea una SparkSession configurada para la nueva infraestructura
    (Spark 4.1.1 + Hadoop 3.4.0 + MinIO).
    """
    
    # Credenciales por defecto o variables de entorno
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    return spark