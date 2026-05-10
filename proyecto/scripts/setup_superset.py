"""
Crea en Superset la conexión a MariaDB, los datasets y el dashboard
de simulación de stock del proyecto smart_inventory.

Uso:
    python3 scripts/setup_superset.py

Requiere que el stack bigdata_stack_platform esté corriendo y que
las tablas de MariaDB estén creadas (ver sql/crear_tablas_servicio_db.sql).
"""

import sys
import requests

SUPERSET_URL = "http://localhost:8088"
USER         = "admin"
PASSWORD     = "admin123"

DB_NAME      = "smart_inventory_mariadb"
DB_URI       = "mysql+pymysql://bigdata_user:bigdata_pass@mariadb:3306/bigdata_db"

DATASETS = [
    {"table": "reporte_simulacion_stock", "label": "Simulación Monte Carlo"},
    {"table": "reporte_forecast",         "label": "Forecast de Demanda"},
]

DASHBOARD_TITLE = "Inventario — Simulación de Stock"


def _session():
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json"})

    r = s.post(f"{SUPERSET_URL}/api/v1/security/login", json={
        "username": USER,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True,
    })
    r.raise_for_status()
    token = r.json()["access_token"]
    s.headers.update({"Authorization": f"Bearer {token}"})

    # CSRF token para operaciones de escritura (POST/PUT/DELETE)
    r = s.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    r.raise_for_status()
    s.headers.update({"X-CSRFToken": r.json()["result"]})

    return s


def _list_all(s, endpoint):
    page, results = 0, []
    while True:
        r = s.get(f"{SUPERSET_URL}/api/v1/{endpoint}/",
                  params={"q": f"(page:{page},page_size:100)"})
        r.raise_for_status()
        data = r.json()
        results.extend(data.get("result", []))
        if len(results) >= data.get("count", 0):
            break
        page += 1
    return results


def get_or_create_database(s):
    for db in _list_all(s, "database"):
        if db["database_name"] == DB_NAME:
            print(f"  Base de datos ya existe (id={db['id']})")
            return db["id"]

    r = s.post(f"{SUPERSET_URL}/api/v1/database/", json={
        "database_name":    DB_NAME,
        "sqlalchemy_uri":   DB_URI,
        "expose_in_sqllab": True,
    })
    r.raise_for_status()
    db_id = r.json()["id"]
    print(f"  Base de datos creada (id={db_id})")
    return db_id


def get_or_create_dataset(s, db_id, table_name):
    for ds in _list_all(s, "dataset"):
        if ds["table_name"] == table_name:
            print(f"    Dataset '{table_name}' ya existe (id={ds['id']})")
            return ds["id"]

    r = s.post(f"{SUPERSET_URL}/api/v1/dataset/", json={
        "database":   db_id,
        "table_name": table_name,
        "schema":     "",
    })
    if not r.ok:
        print(f"    Error creando dataset: {r.status_code} {r.text[:300]}")
        r.raise_for_status()
    ds_id = r.json()["id"]
    print(f"    Dataset '{table_name}' creado (id={ds_id})")
    return ds_id


def create_dashboard(s, title):
    for dash in _list_all(s, "dashboard"):
        if dash["dashboard_title"] == title:
            print(f"  Dashboard '{title}' ya existe (id={dash['id']})")
            return dash["id"]

    r = s.post(f"{SUPERSET_URL}/api/v1/dashboard/", json={
        "dashboard_title": title,
        "published":       True,
    })
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"  Dashboard '{title}' creado (id={dash_id})")
    return dash_id


def create_chart(s, ds_id, chart_cfg):
    for chart in _list_all(s, "chart"):
        if chart["slice_name"] == chart_cfg["slice_name"]:
            print(f"    Chart '{chart_cfg['slice_name']}' ya existe (id={chart['id']})")
            return chart["id"]

    r = s.post(f"{SUPERSET_URL}/api/v1/chart/", json={
        "datasource_id":   ds_id,
        "datasource_type": "table",
        "slice_name":      chart_cfg["slice_name"],
        "viz_type":        chart_cfg["viz_type"],
        "params":          chart_cfg["params"],
    })
    r.raise_for_status()
    chart_id = r.json()["id"]
    print(f"    Chart '{chart_cfg['slice_name']}' creado (id={chart_id})")
    return chart_id


def add_chart_to_dashboard(_s, _dash_id, _chart_ids):
    # En Superset 5+ el layout se gestiona vía position_json (complejo).
    # Los charts quedan disponibles en Charts > List para arrastrarlos al dashboard.
    pass


CHARTS_CONFIG = [
    {
        "slice_name": "Probabilidad de Quiebre por Producto",
        "viz_type":   "bar",
        "params":     '{"metrics":["avg__probabilidad_quiebre"],"groupby":["producto_id"],"row_limit":50}',
    },
    {
        "slice_name": "Demanda P50 vs P95 por Fecha",
        "viz_type":   "line",
        "params":     '{"metrics":["avg__demanda_p50","avg__demanda_p95_critica"],"groupby":[],"granularity_sqla":"fecha","time_range":"No filter"}',
    },
    {
        "slice_name": "Stock Promedio Esperado por Producto",
        "viz_type":   "bar",
        "params":     '{"metrics":["avg__stock_promedio_esperado"],"groupby":["producto_id"],"row_limit":50}',
    },
]


def main():
    print("Conectando a Superset...")
    try:
        s = _session()
    except Exception as e:
        print(f"ERROR: No se pudo conectar a Superset en {SUPERSET_URL}: {e}")
        sys.exit(1)

    print("\n1. Creando conexión a MariaDB...")
    db_id = get_or_create_database(s)

    print("\n2. Creando datasets...")
    ds_simulacion = get_or_create_dataset(s, db_id, "reporte_simulacion_stock")
    get_or_create_dataset(s, db_id, "reporte_forecast")

    print("\n3. Creando dashboard...")
    dash_id = create_dashboard(s, DASHBOARD_TITLE)

    print("\n4. Creando charts...")
    chart_ids = []
    for cfg in CHARTS_CONFIG:
        chart_ids.append(create_chart(s, ds_simulacion, cfg))

    print("\n5. Vinculando charts al dashboard...")
    add_chart_to_dashboard(s, dash_id, chart_ids)

    print(f"\n✅ Setup completo.")
    print(f"   Dashboard : {SUPERSET_URL}/superset/dashboard/{dash_id}/")
    print(f"   Charts    : {SUPERSET_URL}/chart/list/")
    print()
    print("   Paso final: abrí el dashboard → Edit → arrastrá los 3 charts desde")
    print("   el panel 'Your charts & filters' al lienzo del dashboard → Save.")


if __name__ == "__main__":
    main()
