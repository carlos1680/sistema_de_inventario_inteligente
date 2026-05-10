import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from comun.sesion_spark import obtener_sesion_spark
from servicios.save_db import guardar_en_mariadb
from pyspark.ml import PipelineModel
from pyspark.sql.functions import (
    col, lit, randn, greatest, avg,
    percentile_approx, when,
    sum as spark_sum,
)
from pyspark.sql.window import Window

# ── Parámetros de simulación ───────────────────────────────────────────────────
N_SIMS         = 100    # número de escenarios Monte Carlo
VOLATILIDAD    = 0.15   # desviación estándar como fracción de la demanda esperada
STOCK_INICIAL  = 500    # unidades iniciales por producto (sin reposiciones)


def simular(spark, df_pred, n_sims=N_SIMS, volatilidad=VOLATILIDAD, stock_inicial=STOCK_INICIAL):
    """
    Aplica simulación Monte Carlo sobre predicciones de demanda.

    Recibe un DataFrame con columnas (fecha, tienda_id, producto_id, demanda_esperada)
    y devuelve uno con (fecha, tienda_id, producto_id, probabilidad_quiebre,
    demanda_p50, demanda_p95_critica, stock_promedio_esperado).
    """
    sim_ids = spark.range(n_sims).toDF("sim_id")
    df_mc   = df_pred.crossJoin(sim_ids)

    df_mc = df_mc.withColumn(
        "demanda_simulada",
        greatest(
            lit(0.0),
            col("demanda_esperada") * (lit(1.0) + randn() * lit(volatilidad)),
        ),
    )

    w_acum = (
        Window.partitionBy("producto_id", "tienda_id", "sim_id")
              .orderBy("fecha")
              .rowsBetween(Window.unboundedPreceding, 0)
    )
    df_mc = (
        df_mc
        .withColumn("demanda_acumulada", spark_sum("demanda_simulada").over(w_acum))
        .withColumn("stock_proyectado",  lit(float(stock_inicial)) - col("demanda_acumulada"))
        .withColumn("quiebre_stock",     when(col("stock_proyectado") < lit(0.0), 1).otherwise(0))
    )

    return df_mc.groupBy("fecha", "tienda_id", "producto_id").agg(
        avg("quiebre_stock")                        .alias("probabilidad_quiebre"),
        percentile_approx("demanda_simulada", 0.50) .alias("demanda_p50"),
        percentile_approx("demanda_simulada", 0.95) .alias("demanda_p95_critica"),
        avg("stock_proyectado")                     .alias("stock_promedio_esperado"),
    )


def main():
    spark = obtener_sesion_spark("Simulacion_MonteCarlo")

    path_features = "s3a://gold/smart_inventory/dataset_features"
    path_modelo   = "s3a://models/smart_inventory/rf_demanda_v1"
    path_salida   = "s3a://gold/smart_inventory/resultados_montecarlo"
    path_csv      = "/opt/shared/resultados_montecarlo_csv"

    print(f">>> Leyendo features: {path_features}")
    df = spark.read.parquet(path_features)

    print(f">>> Cargando modelo: {path_modelo}")
    modelo = PipelineModel.load(path_modelo)

    df_pred = (
        modelo.transform(df)
              .select("fecha", "tienda_id", "producto_id",
                      col("prediction").alias("demanda_esperada"))
    )

    print(f">>> Iniciando simulación ({N_SIMS} escenarios por fila)...")
    df_resultado = simular(spark, df_pred)

    print(">>> Muestra de resultados:")
    df_resultado.orderBy("producto_id", "fecha").show(10, truncate=False)

    print(f">>> Guardando Parquet en MinIO: {path_salida}")
    df_resultado.write.mode("overwrite").parquet(path_salida)

    print(f">>> Guardando CSV en: {path_csv}")
    df_resultado.orderBy("producto_id", "fecha") \
                .coalesce(1) \
                .write.mode("overwrite") \
                .option("header", "true") \
                .csv(path_csv)

    guardar_en_mariadb(df_resultado, "reporte_simulacion_stock")

    print(f"✅ Simulación Monte Carlo completada ({N_SIMS} escenarios por fila).")


if __name__ == "__main__":
    main()
