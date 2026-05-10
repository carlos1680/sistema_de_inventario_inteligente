import os
import time
import requests

_BASE = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

# MLflow 2.x+ tiene protección contra DNS rebinding que rechaza Host != localhost.
# Forzamos Host: localhost para todas las llamadas desde dentro de la red Docker.
_HEADERS = {"Host": "localhost"}


def _post(endpoint, payload):
    resp = requests.post(
        f"{_BASE}/api/2.0/mlflow/{endpoint}",
        json=payload,
        headers=_HEADERS,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def _get(url, **kwargs):
    resp = requests.get(url, headers=_HEADERS, timeout=10, **kwargs)
    resp.raise_for_status()
    return resp.json()


def get_or_create_experiment(name):
    try:
        return _post("experiments/create", {"name": name})["experiment_id"]
    except requests.HTTPError as e:
        if e.response.status_code == 400:
            data = _get(
                f"{_BASE}/api/2.0/mlflow/experiments/get-by-name",
                params={"experiment_name": name},
            )
            return data["experiment"]["experiment_id"]
        raise


def start_run(experiment_id, run_name):
    data = _post("runs/create", {
        "experiment_id": experiment_id,
        "run_name": run_name,
        "start_time": int(time.time() * 1000),
    })
    return data["run"]["info"]["run_id"]


def log_params(run_id, params):
    for k, v in params.items():
        _post("runs/log-parameter", {"run_id": run_id, "key": k, "value": str(v)})


def log_metrics(run_id, metrics):
    ts = int(time.time() * 1000)
    for k, v in metrics.items():
        _post("runs/log-metric", {"run_id": run_id, "key": k, "value": float(v), "timestamp": ts, "step": 0})


def end_run(run_id, status="FINISHED"):
    _post("runs/update", {
        "run_id": run_id,
        "status": status,
        "end_time": int(time.time() * 1000),
    })
