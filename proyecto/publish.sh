#!/bin/bash
set -e

# ============================================================================
# CONFIGURACIÓN (Ajustada a la nueva Infra)
# ============================================================================
# Ruta base del repo bigdata_stack_platform
BIGDATA_BASE="$HOME/Documentos/repositoriosGithub/personal/bigdata_stack_platform/volumenes"
BIGDATA_SHARED="$BIGDATA_BASE/shared"

# Colores
GREEN='\033[0;32m'
NC='\033[0m'
print_ok() { echo -e "${GREEN}[✓]${NC} $1"; }

# ============================================================================
# 1. PREPARACIÓN DE DATOS (Landing)
# ============================================================================
print_ok "⚙️ Sincronizando datos CSV a Landing..."
# Creamos la carpeta destino en el volumen compartido
LANDING_DIR="$BIGDATA_SHARED/minioshareddata/landing/smart_inventory"
sudo mkdir -p "$LANDING_DIR"

# Copiamos el archivo de ejemplo
if [ -f "data/raw/ventas_ejemplo.csv" ]; then
    sudo cp data/raw/ventas_ejemplo.csv "$LANDING_DIR/"
    print_ok "📄 ventas_ejemplo.csv copiado a Landing."
else
    echo "⚠️ No se encontró data/raw/ventas_ejemplo.csv, saltando copia de datos."
fi

# Permisos para que Spark (UID 185) pueda leer/escribir
sudo chown -R 1000:1000 "$BIGDATA_SHARED"
sudo chmod -R 777 "$BIGDATA_SHARED"

# ============================================================================
# 2. PUBLICACIÓN DE CÓDIGO (Python Scripts)
# ============================================================================
print_ok "📝 Publicando Scripts Python..."

# Destino: /opt/spark/app/smart_inventory (visto desde dentro del container)
# En el host es: .../volumenes/shared/scripts_airflow/smart_inventory
TARGET_SPARK="$BIGDATA_SHARED/scripts_airflow/smart_inventory"

# Limpiamos y recreamos
sudo rm -rf "$TARGET_SPARK"
mkdir -p "$TARGET_SPARK"

# Copiamos todo el contenido de 'src' manteniendo la estructura (bronce, plata, comun...)
# Esto es vital para que funcionen los 'from comun.sesion_spark import ...'
rsync -av --delete src/ "$TARGET_SPARK/"

# ============================================================================
# 3. PUBLICACIÓN DE DAGS (Airflow)
# ============================================================================
print_ok "🌪️ Publicando DAGs..."

TARGET_DAGS="$BIGDATA_SHARED/dags_airflow"
mkdir -p "$TARGET_DAGS"

# Copiamos el DAG
cp dags/dag_C03_flujo_principal.py "$TARGET_DAGS/"

print_ok "🚀 Despliegue completado. Código disponible en el cluster."