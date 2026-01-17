import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


def entrenar_modelo_demanda():
    spark = obtener_sesion_spark("Entrenamiento_Modelo_Demanda")
    path_features = "s3a://gold/smart_inventory/dataset_features"
    path_modelo = "s3a://gold/smart_inventory/modelos/modelo_demanda_lr"

    print(f">>> Cargando dataset desde: {path_features}")
    df = spark.read.parquet(path_features)

    # 1) Castear a double y filtrar filas problemáticas (NULL / NaN)
    feature_cols = ["dia_semana", "es_fin_semana", "mes", "ventas_lag_1", "media_movil_7d"]

    df_preparado = (
        df
        .withColumn("target_ventas_proximo_dia", F.col("target_ventas_proximo_dia").cast("double"))
        .withColumn("ventas_lag_1", F.col("ventas_lag_1").cast("double"))
        .withColumn("media_movil_7d", F.col("media_movil_7d").cast("double"))
    )

    # Filtrar cualquier fila con NULL en label o features usadas
    cols_a_verificar = ["target_ventas_proximo_dia"] + feature_cols
    condicion_not_null = " AND ".join([f"{c} IS NOT NULL" for c in cols_a_verificar])

    df_filtrado = df_preparado.filter(condicion_not_null)

    count_total = df_preparado.count()
    count_filtrado = df_filtrado.count()
    print(f">>> Filas totales: {count_total} | Filas sin NULL en label/features: {count_filtrado}")

    if count_filtrado == 0:
        print("❌ No hay filas suficientes sin NULL/NaN para entrenar el modelo.")
        spark.stop()
        return

    # 2) VectorAssembler
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )
    df_ml = assembler.transform(df_filtrado)

    # 3) Train / Test split
    train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)

    print(">>> Entrenando modelo de Regresión Lineal...")
    lr = LinearRegression(
        featuresCol="features",
        labelCol="target_ventas_proximo_dia"
    )
    model = lr.fit(train_data)

    # 4) Evaluación
    if test_data.count() > 0:
        predictions = model.transform(test_data)
        evaluator = RegressionEvaluator(
            labelCol="target_ventas_proximo_dia",
            predictionCol="prediction",
            metricName="rmse",
        )
        rmse = evaluator.evaluate(predictions)
        print(f">>> Modelo entrenado. RMSE (test): {rmse:.2f}")
    else:
        print("⚠ No hay suficientes datos para conjunto de test, se entrenó solo con train.")

    # 5) Guardar modelo
    model.write().overwrite().save(path_modelo)
    print(f"✅ Modelo guardado exitosamente en {path_modelo}")

    spark.stop()


if __name__ == "__main__":
    entrenar_modelo_demanda()