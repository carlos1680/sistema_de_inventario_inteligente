# Simulación Monte Carlo de Inventario (Capa Oro)

Este módulo implementa una simulación Monte Carlo de demanda e inventario sobre la capa Gold (Oro) del proyecto `smart_inventory`. A partir de un modelo de predicción de demanda entrenado en Spark, se generan múltiples escenarios de ventas futuras para estimar:

- La probabilidad de quiebre de stock por producto y fecha.
- La demanda esperada (p50).
- La demanda en escenario crítico (p95).
- El stock proyectado promedio.

Los resultados se almacenan tanto en MinIO (formato Parquet) como en el filesystem local de Spark (formato CSV legible para humanos).

---

## 1. Objetivo del Módulo

El objetivo principal de este componente es responder preguntas de negocio como:

- ¿Qué tan probable es que me quede sin stock para un producto en una fecha determinada?
- ¿Cuál es la demanda esperada y cuál sería la demanda en un escenario pesimista (alta demanda)?
- ¿Cómo evoluciona el stock proyectado si no se realizan reposiciones?

Para ello, se utiliza:

1. Un modelo de regresión lineal (`LinearRegressionModel`) entrenado previamente en Spark para predecir demanda diaria.
2. Una simulación Monte Carlo con ruido gaussiano sobre la demanda esperada.
3. Un stock inicial simulado (a modo de ejemplo) para calcular el riesgo de quiebre.

---

## 2. Arquitectura y Componentes

### 2.1. Archivo principal

- `src/oro/spark_simulacion_montecarlo.py`

Script Spark (PySpark) que:
- Carga el modelo de demanda desde la capa Gold en MinIO.
- Carga el dataset de features desde la capa Gold.
- Genera escenarios simulados de demanda (Monte Carlo).
- Calcula riesgo de quiebre de stock.
- Escribe resultados en:
  - MinIO: `s3a://gold/smart_inventory/resultados_montecarlo` (Parquet).
  - Filesystem local de Spark: `/opt/shared/resultados_montecarlo_csv` (CSV).

### 2.2. Dependencias clave

- `comun/sesion_spark.py`  
  Crea la sesión de Spark con la configuración necesaria para acceder a MinIO vía `s3a://`.

- Librerías PySpark:
  - `pyspark.sql.functions`
  - `pyspark.sql.window.Window`
  - `pyspark.ml.regression.LinearRegressionModel`
  - `pyspark.ml.feature.VectorAssembler`

---

## 3. Flujo de Datos

### 3.1. Entrada (Gold en MinIO)

- Modelo:
  - `s3a://gold/smart_inventory/modelos/modelo_demanda_lr`

- Dataset de features:
  - `s3a://gold/smart_inventory/dataset_features`

El dataset de features contiene, entre otras, las columnas:

- `producto_id`
- `fecha`
- `dia_semana`
- `es_fin_semana`
- `mes`
- `ventas_lag_1`
- `media_movil_7d`
- (otras columnas derivadas del histórico de ventas)

### 3.2. Procesamiento (Simulación Monte Carlo)

1. Se filtran filas con valores NULL en las columnas de features clave.
2. Se construye el vector de features mediante `VectorAssembler`.
3. Se obtiene una demanda esperada por `producto_id` y `fecha` (columna `prediction` del modelo).
4. Se generan N simulaciones (por defecto, `NUM_SIMULACIONES = 100`) aplicando:
   - Ruido gaussiano alrededor de la demanda esperada, escalado por una volatilidad (`VOLATILIDAD_PCT`).
5. Se acumula la demanda simulada en el tiempo para cada combinación (`producto_id`, `simulacion_id`).
6. Se considera un stock inicial simulado de 500 unidades (ejemplo simple sin reposiciones).
7. Se calcula por escenario y fecha:
   - `stock_proyectado`
   - `quiebre_stock` (1 si el stock proyectado < 0, 0 en caso contrario)

### 3.3. Salida

- MinIO (Parquet, capa Gold):
  - `s3a://gold/smart_inventory/resultados_montecarlo`

- Filesystem local de Spark (CSV legible para humanos):
  - Directorio: `/opt/shared/resultados_montecarlo_csv`
  - Archivos: `part-00000-*.csv` + `_SUCCESS`

---

## 4. Lógica de la Simulación

### 4.1. Predicción base de demanda

Se parte de la predicción del modelo por `producto_id` y `fecha`:

- `demanda_esperada = prediction`

### 4.2. Simulación Monte Carlo

Para cada fila (`producto_id`, `fecha`) se generan `NUM_SIMULACIONES` escenarios:

- Se replica la fila `NUM_SIMULACIONES` veces.
- Para cada réplica se genera un ruido gaussiano:
  - `ruido ~ N(0, VOLATILIDAD_PCT)`
- La demanda simulada se calcula como:

  demanda_simulada = max(0, demanda_esperada * (1 + ruido))

### 4.3. Cálculo de stock y riesgo

- Se parte de un `stock_inicial = 500` unidades por producto (ejemplo simple sin reposiciones).
- Se acumula la demanda simulada por ventana (`producto_id`, `simulacion_id`) ordenada por `fecha`.
- Se calcula:

  demanda_acumulada    = suma(demanda_simulada) por producto y simulación hasta esa fecha  
  stock_proyectado     = stock_inicial - demanda_acumulada  
  quiebre_stock        = 1 si stock_proyectado < 0, 0 en caso contrario  

### 4.4. Agregación final por producto y fecha

A partir de todos los escenarios (simulaciones), se resumen los resultados por `producto_id` y `fecha`:

- `probabilidad_quiebre`  
  Promedio de `quiebre_stock` (entre 0 y 1).  
  Ejemplo: 0.2 → 20% de escenarios se quedaron sin stock ese día.

- `demanda_p50`  
  Mediana (`percentile_approx` 0.5) de la `demanda_simulada`.  
  Es la demanda “esperada”.

- `demanda_p95_critica`  
  Percentil 95 (`percentile_approx` 0.95) de la `demanda_simulada`.  
  Escenario de alta demanda / estrés.

- `stock_promedio_esperado`  
  Promedio del `stock_proyectado` a lo largo de las simulaciones.

---

## 5. Ejemplo de Salida

Ejemplo real de salida en CSV (`/opt/shared/resultados_montecarlo_csv/part-00000-*.csv`):

producto_id,fecha,probabilidad_quiebre,demanda_p50,demanda_p95_critica,stock_promedio_esperado  
PROD_001,2023-01-05,0.0,6.09,7.98,493.86  
PROD_001,2023-02-01,0.0,6.37,8.07,469.18  
PROD_001,2023-03-25,0.0,6.22,7.82,356.78  
PROD_002,2023-01-08,0.0,5.91,7.43,494.15  
PROD_002,2023-02-02,0.0,6.18,8.69,462.34  
...

Interpretación rápida:

- `probabilidad_quiebre = 0.0`  
  No se observó quiebre de stock en ninguno de los escenarios simulados para esa fecha y producto.

- `demanda_p50 ≈ 6`  
  Demanda “normal” esperada de unas 6 unidades.

- `demanda_p95_critica ≈ 8`  
  En un escenario exigente, podría llegarse a vender alrededor de 8 unidades.

- `stock_promedio_esperado`  
  Va disminuyendo con el tiempo a medida que se acumulan ventas, dado que el stock inicial se fija en 500 y no se modelan reposiciones.

---

## 6. Ejecución

### 6.1. Ejecución como script de Spark

Dentro del contenedor / entorno donde corre Spark:

spark-submit \
  --master local[*] \
  src/oro/spark_simulacion_montecarlo.py

Asegúrate de que:

- `comun/sesion_spark.py` está accesible en el `PYTHONPATH`.
- La sesión de Spark está configurada con las credenciales/endpoint correctos para MinIO (`s3a://`).

### 6.2. Ejecución vía Airflow

En el DAG principal de `smart_inventory` (por ejemplo, `dag_C03_inventario_inteligente_principal`), este script puede ser invocado mediante un operador tipo SparkSubmit, apuntando al archivo:

- `src/oro/spark_simulacion_montecarlo.py`

---

## 7. Ubicaciones de los Resultados

- Resultados analíticos (Parquet, Gold / MinIO):

  s3a://gold/smart_inventory/resultados_montecarlo

- Resultados legibles para humanos (CSV, filesystem local de Spark):

  /opt/shared/resultados_montecarlo_csv/part-00000-*.csv

Estos CSV pueden abrirse con herramientas como Excel, LibreOffice, o cargarse en otra base de datos para análisis adicional.

---

## 8. Próximos Pasos / Extensiones

Ideas de extensión del módulo:

- Sustituir el stock fijo (500 unidades) por un stock real proveniente de otra tabla Gold (`stock_actual` por producto y tienda).
- Incorporar reposiciones programadas o ventanas de reabastecimiento según `dias_plazo_proveedor`.
- Exponer estos resultados en un dashboard de BI (por ejemplo, Apache Superset) para:
  - Ver la evolución de la probabilidad de quiebre por producto.
  - Identificar productos con sobrestock / substock.
- Integrar este resultado con un motor de recomendación de órdenes de compra (sugerencia de reabastecimiento).