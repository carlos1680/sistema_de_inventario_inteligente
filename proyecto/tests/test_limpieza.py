import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from datetime import date
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

from plata.spark_limpieza_datos_ventas import limpiar_ventas


@pytest.fixture
def df_ventas(spark):
    schema = StructType([
        StructField("fecha",           StringType(),  True),
        StructField("producto_id",     StringType(),  True),
        StructField("tienda_id",       StringType(),  True),
        StructField("cantidad_vendida",IntegerType(), True),
        StructField("categoria",       StringType(),  True),
    ])
    rows = [
        ("2023-01-01", "P1", "T1",  5, "Electronica"),
        ("2023-01-02", "P1", "T1",  0, None),           # categoria nula
        ("2023-01-03", "P1", "T1", -3, "Electronica"),  # cantidad negativa
        ("2023-01-04", "P2", "T1",  8, "Ropa"),
    ]
    return spark.createDataFrame(rows, schema)


def test_fecha_se_convierte_a_date(spark, df_ventas):
    resultado = limpiar_ventas(df_ventas)
    tipo_fecha = dict(resultado.dtypes)["fecha"]
    assert tipo_fecha == "date", f"Se esperaba 'date', se obtuvo '{tipo_fecha}'"


def test_cantidad_negativa_se_filtra(spark, df_ventas):
    resultado = limpiar_ventas(df_ventas)
    negativos = resultado.filter(resultado.cantidad_vendida < 0).count()
    assert negativos == 0


def test_cantidad_cero_se_conserva(spark, df_ventas):
    resultado = limpiar_ventas(df_ventas)
    ceros = resultado.filter(resultado.cantidad_vendida == 0).count()
    assert ceros == 1


def test_categoria_nula_se_rellena(spark, df_ventas):
    resultado = limpiar_ventas(df_ventas)
    nulos = resultado.filter(resultado.categoria.isNull()).count()
    assert nulos == 0


def test_categoria_nula_se_rellena_con_valor_correcto(spark, df_ventas):
    resultado = limpiar_ventas(df_ventas)
    fila = resultado.filter(resultado.producto_id == "P1") \
                    .filter(resultado.cantidad_vendida == 0) \
                    .first()
    assert fila["categoria"] == "Sin Categoria"


def test_registros_validos_no_se_alteran(spark, df_ventas):
    resultado = limpiar_ventas(df_ventas)
    fila = resultado.filter(resultado.producto_id == "P2").first()
    assert fila["cantidad_vendida"] == 8
    assert fila["categoria"] == "Ropa"
    assert fila["fecha"] == date(2023, 1, 4)
