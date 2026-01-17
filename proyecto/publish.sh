#!/bin/bash
set -e

# ====
# CONFIGURACIÓN DEL STACK
# ====
BIGDATA_BASE="$HOME/Documentos/docker-compose-contenedores/bigdata/volumenes"

# Path para scripts/dags (shared)
BIGDATA_SHARED="$BIGDATA_BASE/shared"

# Path para el bucket de MinIO (data real que ve MinIO)
BIGDATA_MINIO_DATA="$BIGDATA_BASE/minio/data"

# Path para el volumen shared_minio (montado como /shareddata en contenedores)
BIGDATA_MINIO_SHARED="$BIGDATA_SHARED/minio/data"

PROJECT_NAME="smart_inventory"

# Paths locales del proyecto
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$PROJECT_ROOT/src"
DAGS_DIR="$PROJECT_ROOT/dags"
DATA_RAW_DIR="$PROJECT_ROOT/data/raw"
SQL_DIR="$PROJECT_ROOT/sql"

# Colores para la terminal
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_ok()    { echo -e "${GREEN}[✓]${NC} $1"; }
print_warn()  { echo -e "${YELLOW}[!]${NC} $1"; }
print_err()   { echo -e "${RED}[✗]${NC} $1"; }

# ====
# INFRAESTRUCTURA (MinIO)
# ====
prepare_infra() {
    print_ok "Preparando carpetas en MinIO para $PROJECT_NAME..."

    # Directorios en el bucket real de MinIO (./volumenes/minio/data)
    DIRS_MINIO_DATA=(
        "$BIGDATA_MINIO_DATA/bronze/$PROJECT_NAME"
        "$BIGDATA_MINIO_DATA/silver/$PROJECT_NAME"
        "$BIGDATA_MINIO_DATA/gold/$PROJECT_NAME"
    )

    # Directorios en el volumen shared_minio (./volumenes/shared/minio/data)
    # Útil si querés compartir archivos entre contenedores sin pasar por S3
    DIRS_MINIO_SHARED=(
        "$BIGDATA_MINIO_SHARED/$PROJECT_NAME"
    )

    # Crear directorios en el bucket de MinIO
    for d in "${DIRS_MINIO_DATA[@]}"; do
        if [ ! -d "$d" ]; then
            print_warn "Creando directorio en bucket MinIO: $d"
            sudo mkdir -p "$d"
        fi
        # Permisos para que el usuario 1000 (Spark/Airflow) pueda operar
        sudo chmod -R 777 "$d"
        sudo chown -R 1000:1000 "$d"
    done

    # Crear directorios en shared_minio (opcional, para intercambio directo)
    for d in "${DIRS_MINIO_SHARED[@]}"; do
        if [ ! -d "$d" ]; then
            print_warn "Creando directorio en shared_minio: $d"
            sudo mkdir -p "$d"
        fi
        sudo chmod -R 777 "$d"
        sudo chown -R 1000:1000 "$d"
    done
}

# ====
# PUBLICAR SCRIPTS DE SPARK
# ====
publish_spark() {
    print_ok "Publicando scripts de Spark (src/)..."
    
    # Destino en el volumen compartido del stack
    TARGET="$BIGDATA_SHARED/scripts_airflow/$PROJECT_NAME"
    mkdir -p "$TARGET"

    if [ -d "$SRC_DIR" ]; then
        # rsync mantiene la estructura de subcarpetas (bronce, plata, oro, comun, servicios)
        rsync -av --delete "$SRC_DIR/" "$TARGET/"
        
        print_ok "Ajustando permisos de ejecución para scripts spark_*.py..."
        find "$TARGET" -type f -name "*.py" -exec chmod 666 {} \;
    else
        print_err "Error: No se encontró la carpeta $SRC_DIR"
        exit 1
    fi
}

# ====
# PUBLICAR DAGS
# ====
publish_dags() {
    print_ok "Publicando DAGs de Airflow (dags/)..."
    
    TARGET="$BIGDATA_SHARED/dags_airflow"
    mkdir -p "$TARGET"

    if [ -d "$DAGS_DIR" ]; then
        # Copiamos los archivos dag_*.py
        rsync -av "$DAGS_DIR/" "$TARGET/"
        
        print_ok "Ajustando permisos para los DAGs..."
        find "$TARGET" -type f -name "dag_*.py" -exec chmod 666 {} \;
    else
        print_warn "No se encontraron DAGs en $DAGS_DIR"
    fi
}

# ====
# PUBLICAR DATOS INICIALES
# ====
publish_data() {
    print_ok "Publicando datos crudos a Landing Zone (Shared)..."
    
    # Destino: volumen compartido que Spark ve como path local
    DEST_LANDING="$BIGDATA_MINIO_SHARED/$PROJECT_NAME/landing"
    mkdir -p "$DEST_LANDING"

    if [ -f "$DATA_RAW_DIR/ventas_ejemplo.csv" ]; then
        cp "$DATA_RAW_DIR/ventas_ejemplo.csv" "$DEST_LANDING/"
        print_ok "Archivo ventas_ejemplo.csv copiado a Landing: $DEST_LANDING"
    else
        print_warn "No se encontró ventas_ejemplo.csv"
    fi
}

# ====
# PUBLICAR SQL
# ====
publish_sql() {
    print_ok "Ejecutando scripts SQL en MariaDB..."
    
    if [ -f "$SQL_DIR/crear_tablas_servicio_db.sql" ]; then
        # Ejecutar SQL en el contenedor mariadb
        docker exec -i mariadb mariadb -u root -prootpass < "$SQL_DIR/crear_tablas_servicio_db.sql"
        print_ok "Script SQL ejecutado correctamente"
    else
        print_warn "No se encontró el archivo SQL en $SQL_DIR"
    fi
}

# ====
# PUBLICAR NOTEBOOKS
# ====
publish_notebooks() {
    print_ok "Publicando notebooks en JupyterLab..."
    
    # Destino dentro del volumen de JupyterLab
    TARGET_JUPYTER="$BIGDATA_BASE/jupyterlab/inventario"
    mkdir -p "$TARGET_JUPYTER"

    if [ -d "$PROJECT_ROOT/notebooks" ]; then
        # Copiamos los notebooks al volumen de Jupyter
        rsync -av --delete "$PROJECT_ROOT/notebooks/" "$TARGET_JUPYTER/"
        
        # Aseguramos permisos para que el usuario jovyan (1000) pueda editarlos
        sudo chmod -R 777 "$TARGET_JUPYTER"
        sudo chown -R 1000:1000 "$TARGET_JUPYTER"
        print_ok "Notebooks actualizados en $TARGET_JUPYTER"
    else
        print_warn "No se encontró la carpeta notebooks/ en el proyecto"
    fi
}

# ====
# EJECUCIÓN PRINCIPAL
# ====
main() {
    echo "------------------------------------------"
    echo "  SISTEMA DE INVENTARIO INTELIGENTE - DEPLOY"
    echo "------------------------------------------"

    prepare_infra
    publish_spark
    publish_dags
    publish_data
    publish_sql
    publish_notebooks

    echo "------------------------------------------"
    print_ok "Proceso de publicación completado."
    echo ""
    echo "📊 Accesos:"
    echo "   Airflow:  http://localhost:8090"
    echo "   MinIO:    http://localhost:9001"
    echo "   Superset: http://localhost:8088"
    echo "------------------------------------------"
}

main