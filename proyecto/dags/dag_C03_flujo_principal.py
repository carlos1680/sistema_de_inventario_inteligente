from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "carlos_piriz",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Configuración de Rutas en el Contenedor Spark
# Nota: 'smart_inventory' viene de como lo copiamos en publish.sh
BASE_APP_PATH = "/opt/spark/app/smart_inventory"

with DAG(
    dag_id="dag_C03_inventario_inteligente_principal",
    default_args=default_args,
    description="Pipeline E2E Inventario (Spark 4.1.1 + Docker Bake-in)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["inventario", "spark", "minio", "mariadb"],
) as dag:

    # --- COMANDO BASE ---
    # Usamos la configuración ganadora:
    # 1. HOME=/tmp (para permisos)
    # 2. ivy en /tmp (para cache)
    # 3. SIN --packages (usamos las libs quemadas en la imagen Docker)
    SPARK_SUBMIT_CMD = f"""
        docker exec -e HOME=/tmp spark-master /opt/spark/bin/spark-submit \\
          --master spark://spark-master:7077 \\
          --conf spark.eventLog.enabled=true \\
          --conf spark.eventLog.dir=file:///tmp/spark-events \\
          --conf spark.jars.ivy=/tmp/.ivy2 \\
          --conf spark.sql.parquet.enableVectorizedReader=false \\
    """

    # 1) Ingesta: Landing (CSV) -> Bronze (Parquet)
    ingesta_bronce = BashOperator(
        task_id="spark_ingesta_bronce",
        bash_command=f"""
            set -e
            echo "🚀 [1/5] Ingesta Landing -> Bronce..."
            {SPARK_SUBMIT_CMD} {BASE_APP_PATH}/bronce/spark_ingreso_datos_crudos.py
        """,
    )

    # 2) Limpieza: Bronze -> Silver
    limpieza_plata = BashOperator(
        task_id="spark_limpieza_plata",
        bash_command=f"""
            set -e
            echo "🚀 [2/5] Limpieza Bronce -> Plata..."
            {SPARK_SUBMIT_CMD} {BASE_APP_PATH}/plata/spark_limpieza_datos_ventas.py
        """,
    )

    # 3) Feature Engineering: Silver -> Gold (Features)
    features_oro = BashOperator(
        task_id="spark_features_oro",
        bash_command=f"""
            set -e
            echo "🚀 [3/5] Ingeniería de Características Plata -> Oro..."
            {SPARK_SUBMIT_CMD} {BASE_APP_PATH}/oro/spark_ingenieria_caracteristicas.py
        """,
    )

    # 4) Entrenamiento: Gold -> Modelo
    entrenamiento_oro = BashOperator(
        task_id="spark_entrenamiento_oro",
        bash_command=f"""
            set -e
            echo "🚀 [4/5] Entrenamiento Modelo de Demanda..."
            {SPARK_SUBMIT_CMD} {BASE_APP_PATH}/oro/spark_entrenamiento_modelo_de_demanda.py
        """,
    )

    # 5) Simulación Monte Carlo y Guardado DB
    simulacion_montecarlo = BashOperator(
        task_id="spark_simulacion_montecarlo",
        bash_command=f"""
            set -e
            echo "🚀 [5/5] Simulación Monte Carlo y Exportación..."
            {SPARK_SUBMIT_CMD} {BASE_APP_PATH}/oro/spark_simulacion_montecarlo.py
        """,
    )

    # Definición de dependencias
    ingesta_bronce >> limpieza_plata >> features_oro >> entrenamiento_oro >> simulacion_montecarlo