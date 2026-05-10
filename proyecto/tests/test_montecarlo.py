import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

from oro.spark_simulacion_montecarlo import simular

N_SIMS_TEST = 20  # menos simulaciones para que los tests sean rápidos


@pytest.fixture
def df_pred(spark):
    schema = StructType([
        StructField("fecha",           DateType(),   True),
        StructField("tienda_id",       StringType(), True),
        StructField("producto_id",     StringType(), True),
        StructField("demanda_esperada",DoubleType(), True),
    ])
    rows = [
        (date(2023, 1, d), "T1", "P1", float(10))
        for d in range(1, 6)
    ]
    return spark.createDataFrame(rows, schema)


def test_resultado_tiene_una_fila_por_fecha(spark, df_pred):
    resultado = simular(spark, df_pred, n_sims=N_SIMS_TEST)
    assert resultado.count() == df_pred.count()


def test_columnas_de_salida_presentes(spark, df_pred):
    resultado = simular(spark, df_pred, n_sims=N_SIMS_TEST)
    for col in ["probabilidad_quiebre", "demanda_p50", "demanda_p95_critica", "stock_promedio_esperado"]:
        assert col in resultado.columns


def test_demanda_simulada_no_negativa(spark, df_pred):
    # demanda_p50 y demanda_p95 deben ser >= 0 (ruido gaussiano truncado a 0)
    resultado = simular(spark, df_pred, n_sims=N_SIMS_TEST)
    negativos = resultado.filter(
        (resultado.demanda_p50 < 0) | (resultado.demanda_p95_critica < 0)
    ).count()
    assert negativos == 0


def test_p95_mayor_o_igual_a_p50(spark, df_pred):
    resultado = simular(spark, df_pred, n_sims=N_SIMS_TEST)
    violaciones = resultado.filter(
        resultado.demanda_p95_critica < resultado.demanda_p50
    ).count()
    assert violaciones == 0


def test_probabilidad_quiebre_entre_0_y_1(spark, df_pred):
    resultado = simular(spark, df_pred, n_sims=N_SIMS_TEST)
    fuera_de_rango = resultado.filter(
        (resultado.probabilidad_quiebre < 0) | (resultado.probabilidad_quiebre > 1)
    ).count()
    assert fuera_de_rango == 0


def test_stock_alto_implica_quiebre_cero(spark, df_pred):
    # Con stock_inicial=10_000 y demanda=10 en 5 días, no debe haber quiebre
    resultado = simular(spark, df_pred, n_sims=N_SIMS_TEST, stock_inicial=10_000)
    quiebres = resultado.filter(resultado.probabilidad_quiebre > 0).count()
    assert quiebres == 0


def test_stock_bajo_implica_quiebre(spark, df_pred):
    # Con stock_inicial=1 y demanda=10 por día, debe haber quiebre en todos
    resultado = simular(spark, df_pred, n_sims=N_SIMS_TEST, stock_inicial=1)
    sin_quiebre = resultado.filter(resultado.probabilidad_quiebre == 0).count()
    assert sin_quiebre == 0
