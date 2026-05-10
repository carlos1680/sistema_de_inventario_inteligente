import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from comun.minio_utils import garantizar_bucket
from comun.mlflow_client import get_or_create_experiment, start_run, log_params, log_metrics, end_run
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, lead, percentile_approx
from pyspark.sql.window import Window



def entrenar_modelo_demanda():
    garantizar_bucket("models")

    spark = obtener_sesion_spark("Entrenamiento_Modelo_Demanda")

    path_features    = "s3a://gold/smart_inventory/dataset_features"
    path_model       = "s3a://models/smart_inventory/rf_demanda_v1"

    print(f">>> Leyendo features: {path_features}")
    df = spark.read.parquet(path_features)

    # ── Target: ventas del día siguiente ──────────────────────────────────────
    w = Window.partitionBy("tienda_id", "producto_id").orderBy("fecha")
    df_ml = (
        df.withColumn("target", lead("cantidad_vendida", 1).over(w))
          .na.drop()
          .withColumn("target", col("target").cast("double"))
    )

    # ── Split temporal (evita data leakage en series de tiempo) ───────────────
    # Usamos el percentil 80 de fechas como corte, no un random split.
    cutoff = df_ml.select(percentile_approx("fecha", 0.8)).first()[0]
    train  = df_ml.filter(col("fecha") <= cutoff)
    test   = df_ml.filter(col("fecha") >  cutoff)

    n_train = train.count()
    n_test  = test.count()
    print(f">>> Split temporal — corte: {cutoff} | Train: {n_train} | Test: {n_test}")

    # ── Features ──────────────────────────────────────────────────────────────
    feature_cols = [
        "precio_unitario",
        "en_promocion",
        "ventas_lag_1",
        "ventas_lag_7",
        "promedio_movil_7",
        "dia_semana",
        "mes",
    ]

    NUM_TREES  = 20
    MAX_DEPTH  = 5
    SEED       = 42

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    rf        = RandomForestRegressor(
        featuresCol="features",
        labelCol="target",
        numTrees=NUM_TREES,
        maxDepth=MAX_DEPTH,
        seed=SEED,
    )
    pipeline = Pipeline(stages=[assembler, rf])

    # ── MLflow tracking ───────────────────────────────────────────────────────
    experiment_id = get_or_create_experiment("smart_inventory_demand_forecast")
    run_id        = start_run(experiment_id, "rf_demanda_v1")
    print(f">>> MLflow run iniciado: {run_id}")

    try:
        log_params(run_id, {
            "model":          "RandomForestRegressor",
            "num_trees":      NUM_TREES,
            "max_depth":      MAX_DEPTH,
            "seed":           SEED,
            "split_type":     "temporal_p80",
            "cutoff_date":    str(cutoff),
            "n_train":        n_train,
            "n_test":         n_test,
            "features":       ",".join(feature_cols),
        })

        print(">>> Entrenando modelo...")
        model = pipeline.fit(train)

        # ── Métricas ──────────────────────────────────────────────────────────
        preds = model.transform(test)

        def evaluar(metrica):
            return RegressionEvaluator(
                labelCol="target", predictionCol="prediction", metricName=metrica
            ).evaluate(preds)

        rmse = evaluar("rmse")
        mae  = evaluar("mae")
        r2   = evaluar("r2")

        print(f"✅ RMSE: {rmse:.4f} | MAE: {mae:.4f} | R²: {r2:.4f}")

        # ── Feature importance ────────────────────────────────────────────────
        rf_model       = model.stages[-1]
        importances    = rf_model.featureImportances.toArray()
        fi_metrics     = {f"fi_{feat}": float(imp) for feat, imp in zip(feature_cols, importances)}

        log_metrics(run_id, {"rmse": rmse, "mae": mae, "r2": r2, **fi_metrics})

        # ── Guardar modelo ────────────────────────────────────────────────────
        print(f">>> Guardando modelo en: {path_model}")
        model.write().overwrite().save(path_model)

        log_params(run_id, {"artifact_path": path_model})
        end_run(run_id, status="FINISHED")
        print(f"✅ MLflow run cerrado: {run_id}")

    except Exception as e:
        end_run(run_id, status="FAILED")
        raise e


if __name__ == "__main__":
    entrenar_modelo_demanda()
