import sys
import os
import boto3
from botocore.exceptions import ClientError

# Configuración de paths para importar 'comun'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, lead
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

def entrenar_modelo_demanda():
    # 0. Garantizar bucket para modelos
    garantizar_bucket("models")

    spark = obtener_sesion_spark("Entrenamiento_Modelo_Demanda")
    
    path_gold_features = "s3a://gold/smart_inventory/dataset_features"
    path_model_output = "s3a://models/smart_inventory/rf_demanda_v1"

    print(f">>> Leyendo dataset de features: {path_gold_features}")
    df = spark.read.parquet(path_gold_features)

    # 1. CREAR EL TARGET (Corrección del error)
    # Queremos predecir la venta del día siguiente.
    # Usamos 'lead' para traer el valor de mañana a la fila de hoy.
    print(">>> Creando columna target (Ventas Próximo Día)...")
    w = Window.partitionBy("tienda_id", "producto_id").orderBy("fecha")
    
    df_con_target = df.withColumn("target_ventas_proximo_dia", lead("cantidad_vendida", 1).over(w)) \
                      .na.drop() # Eliminamos la última fila de cada serie (no tiene target)

    # Convertir a double para ML
    df_ml = df_con_target.withColumn("target_ventas_proximo_dia", col("target_ventas_proximo_dia").cast("double"))

    # 2. Definir Features para el modelo
    # Usamos las que creamos en el paso anterior + algunas originales utiles
    feature_cols = [
        "precio_unitario", 
        "en_promocion", 
        "ventas_lag_1", 
        "ventas_lag_7", 
        "promedio_movil_7",
        "dia_semana",
        "mes"
    ]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # 3. Split Train/Test
    # Lo ideal es split temporal, pero para este ejemplo simple usamos random
    train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)
    
    print(f">>> Registros Train: {train_data.count()} | Test: {test_data.count()}")

    # 4. Entrenar Random Forest
    rf = RandomForestRegressor(featuresCol="features", labelCol="target_ventas_proximo_dia", numTrees=20)
    
    pipeline = Pipeline(stages=[assembler, rf])
    
    print(">>> Entrenando modelo...")
    model = pipeline.fit(train_data)

    # 5. Evaluar
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="target_ventas_proximo_dia", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"✅ Modelo Entrenado. RMSE en Test: {rmse}")

    # 6. Guardar Modelo
    print(f">>> Guardando modelo en: {path_model_output}")
    model.write().overwrite().save(path_model_output)

if __name__ == "__main__":
    entrenar_modelo_demanda()