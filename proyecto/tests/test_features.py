import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from datetime import date
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType

from oro.spark_ingenieria_caracteristicas import generar_features


@pytest.fixture
def df_silver(spark):
    """Serie de 10 días consecutivos para un producto, suficiente para calcular lag_7."""
    schema = StructType([
        StructField("fecha",            DateType(),    True),
        StructField("tienda_id",        StringType(),  True),
        StructField("producto_id",      StringType(),  True),
        StructField("cantidad_vendida", IntegerType(), True),
        StructField("precio_unitario",  DoubleType(),  True),
        StructField("en_promocion",     IntegerType(), True),
        StructField("categoria",        StringType(),  True),
    ])
    rows = [
        (date(2023, 1, d), "T1", "P1", d * 2, 10.0, 0, "Cat")
        for d in range(1, 11)
    ]
    return spark.createDataFrame(rows, schema)


def test_columnas_nuevas_presentes(spark, df_silver):
    resultado = generar_features(df_silver)
    cols = resultado.columns
    for c in ["ventas_lag_1", "ventas_lag_7", "promedio_movil_7", "dia_semana", "mes"]:
        assert c in cols, f"Falta columna: {c}"


def test_primeros_dias_eliminados_por_na_drop(spark, df_silver):
    # Con 10 días y lag_7, los primeros 7 días no tienen ventas_lag_7 → na.drop() los elimina
    resultado = generar_features(df_silver)
    assert resultado.count() == 3  # solo días 8, 9, 10 tienen lag_7


def test_lag_1_es_correcto(spark, df_silver):
    resultado = generar_features(df_silver)
    # Día 8 → cantidad_vendida=16; lag_1 debe ser el día 7 → cantidad_vendida=14
    fila = resultado.filter(resultado.fecha == date(2023, 1, 8)).first()
    assert fila["ventas_lag_1"] == 14


def test_lag_7_es_correcto(spark, df_silver):
    # Día 8 → lag_7 debe ser el día 1 → cantidad_vendida=2
    resultado = generar_features(df_silver)
    fila = resultado.filter(resultado.fecha == date(2023, 1, 8)).first()
    assert fila["ventas_lag_7"] == 2


def test_mes_es_correcto(spark, df_silver):
    resultado = generar_features(df_silver)
    for fila in resultado.collect():
        assert fila["mes"] == 1


def test_promedio_movil_no_nulo(spark, df_silver):
    resultado = generar_features(df_silver)
    nulos = resultado.filter(resultado.promedio_movil_7.isNull()).count()
    assert nulos == 0
