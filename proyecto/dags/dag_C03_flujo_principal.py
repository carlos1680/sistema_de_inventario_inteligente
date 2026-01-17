from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "carlos_piriz",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Config Docker + Spark (igual que en el ejemplo de fraud_pipeline)
DOCKER_BIN = "docker"
SPARK_CONTAINER_NAME = "spark-master"
SPARK_SUBMIT_PATH = "/opt/spark/bin/spark-submit"
SPARK_MASTER_URL = "spark://spark-master:7077"

# Ruta dentro del contenedor spark-master
# (mapeada desde shared_scripts_airflow:/opt/spark/app)
PROJECT_NAME = "smart_inventory"
BASE_APP_PATH = f"/opt/spark/app/{PROJECT_NAME}"

with DAG(
    dag_id="dag_C03_inventario_inteligente_principal",
    default_args=default_args,
    description="Pipeline E2E de inventario inteligente (Landing -> Bronze -> Silver -> Gold)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["inventario", "spark", "minio", "mariadb"],
) as dag:

    # 1) Ingesta: Landing (shared_minio) -> Bronze (bucket MinIO via s3a)
    ingesta_bronce = BashOperator(
        task_id="spark_ingesta_bronce",
        bash_command=f"""
            set -e
            echo "🚀 Ejecutando ingesta Landing -> Bronze dentro de {SPARK_CONTAINER_NAME}..."

            {DOCKER_BIN} exec {SPARK_CONTAINER_NAME} {SPARK_SUBMIT_PATH} \
              --master {SPARK_MASTER_URL} \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
              {BASE_APP_PATH}/bronce/spark_ingreso_datos_crudos.py
        """,
    )

    # 2) Limpieza (Bronze -> Silver)  [placeholder, cuando tengas la lógica]
    limpieza_plata = BashOperator(
        task_id="spark_limpieza_plata",
        bash_command=f"""
            set -e
            echo "🚀 Ejecutando limpieza Bronze -> Silver dentro de {SPARK_CONTAINER_NAME}..."

            {DOCKER_BIN} exec {SPARK_CONTAINER_NAME} {SPARK_SUBMIT_PATH} \
              --master {SPARK_MASTER_URL} \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
              {BASE_APP_PATH}/plata/spark_limpieza_datos_ventas.py
        """,
    )

    # 3) Features (Silver -> Oro) [placeholder]
    features_oro = BashOperator(
        task_id="spark_features_oro",
        bash_command=f"""
            set -e
            echo "🚀 Ejecutando ingeniería de características Silver -> Oro dentro de {SPARK_CONTAINER_NAME}..."

            {DOCKER_BIN} exec {SPARK_CONTAINER_NAME} {SPARK_SUBMIT_PATH} \
              --master {SPARK_MASTER_URL} \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
              {BASE_APP_PATH}/oro/spark_ingenieria_caracteristicas.py
        """,
    )

    # 4) Entrenamiento modelo demanda [placeholder]
    entrenamiento_oro = BashOperator(
        task_id="spark_entrenamiento_oro",
        bash_command=f"""
            set -e
            echo "🚀 Ejecutando entrenamiento del modelo de demanda dentro de {SPARK_CONTAINER_NAME}..."

            {DOCKER_BIN} exec {SPARK_CONTAINER_NAME} {SPARK_SUBMIT_PATH} \
              --master {SPARK_MASTER_URL} \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
              {BASE_APP_PATH}/oro/spark_entrenamiento_modelo_de_demanda.py
        """,
    )

    # 5) Simulación Monte Carlo [placeholder]
    simulacion_montecarlo = BashOperator(
        task_id="spark_simulacion_montecarlo",
        bash_command=f"""
            set -e
            echo "🚀 Ejecutando simulación de Monte Carlo dentro de {SPARK_CONTAINER_NAME}..."

            {DOCKER_BIN} exec {SPARK_CONTAINER_NAME} {SPARK_SUBMIT_PATH} \
              --master {SPARK_MASTER_URL} \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
              {BASE_APP_PATH}/oro/spark_simulacion_montecarlo.py
        """,
    )

    # Flujo lineal
    ingesta_bronce >> limpieza_plata >> features_oro >> entrenamiento_oro >> simulacion_montecarlo