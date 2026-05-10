import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from unittest.mock import patch, MagicMock
import requests

from comun.mlflow_client import (
    get_or_create_experiment,
    start_run,
    log_params,
    log_metrics,
    end_run,
)


def _mock_post(json_response):
    mock = MagicMock()
    mock.json.return_value = json_response
    mock.raise_for_status = MagicMock()
    return mock


def _mock_get(json_response):
    mock = MagicMock()
    mock.json.return_value = json_response
    mock.raise_for_status = MagicMock()
    return mock


@patch("comun.mlflow_client.requests.post")
def test_get_or_create_experiment_nuevo(mock_post):
    mock_post.return_value = _mock_post({"experiment_id": "42"})
    exp_id = get_or_create_experiment("mi_experimento")
    assert exp_id == "42"


@patch("comun.mlflow_client.requests.get")
@patch("comun.mlflow_client.requests.post")
def test_get_or_create_experiment_existente(mock_post, mock_get):
    # POST devuelve 400 (experimento ya existe)
    error_response = MagicMock()
    error_response.status_code = 400
    http_error = requests.HTTPError(response=error_response)
    mock_post.return_value.raise_for_status.side_effect = http_error

    mock_get.return_value = _mock_get({"experiment": {"experiment_id": "99"}})

    exp_id = get_or_create_experiment("mi_experimento")
    assert exp_id == "99"


@patch("comun.mlflow_client.requests.post")
def test_start_run_devuelve_run_id(mock_post):
    mock_post.return_value = _mock_post({"run": {"info": {"run_id": "abc123"}}})
    run_id = start_run("42", "test_run")
    assert run_id == "abc123"


@patch("comun.mlflow_client.requests.post")
def test_log_params_llama_una_vez_por_param(mock_post):
    mock_post.return_value = _mock_post({})
    log_params("run1", {"a": 1, "b": 2, "c": 3})
    assert mock_post.call_count == 3


@patch("comun.mlflow_client.requests.post")
def test_log_metrics_llama_una_vez_por_metrica(mock_post):
    mock_post.return_value = _mock_post({})
    log_metrics("run1", {"rmse": 0.5, "mae": 0.3})
    assert mock_post.call_count == 2


@patch("comun.mlflow_client.requests.post")
def test_end_run_status_finished(mock_post):
    mock_post.return_value = _mock_post({})
    end_run("run1", status="FINISHED")
    payload = mock_post.call_args[1]["json"]
    assert payload["status"] == "FINISHED"
    assert payload["run_id"] == "run1"


@patch("comun.mlflow_client.requests.post")
def test_end_run_status_failed(mock_post):
    mock_post.return_value = _mock_post({})
    end_run("run1", status="FAILED")
    payload = mock_post.call_args[1]["json"]
    assert payload["status"] == "FAILED"
