"""
Fixtures for testing the tasks defined inside the @dag function of controller_dag.py.

The tasks are nested in `pipeline_controller()`, so they can't be imported directly.
`controller.tasks["get_watermark"]` etc. give you the plain, undecorated Python functions.

Two modes, chosen automatically:
  * Airflow installed (e.g. inside your Airflow docker image): the real DAG is built
    and each task's `python_callable` is extracted.
  * Airflow NOT installed: tiny stand-ins for `airflow.sdk` and TriggerDagRunOperator are
    registered in sys.modules, so the file imports and the task functions are captured.
"""
import importlib
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# --- adjust to your project layout ------------------------------------------
DAGS_DIR = Path(__file__).resolve().parents[2] / "airflow" / "dags"   # folder holding controller_dag.py and utils.py
DAG_MODULE = "controller_dag"
# -----------------------------------------------------------------------------

try:
    import airflow.sdk  # noqa: F401

    AIRFLOW_INSTALLED = True
except ImportError:
    AIRFLOW_INSTALLED = False


def _install_airflow_stubs():
    """Register fake airflow modules; returns the registry {task_name: raw function}."""
    registry = {}

    def task(fn=None, **_kwargs):
        def decorate(f):
            registry[f.__name__] = f
            # in a real DAG, calling a task returns an XComArg placeholder, not a result
            return MagicMock(name=f"xcomarg:{f.__name__}")

        return decorate(fn) if fn is not None else decorate

    def dag(fn=None, **_kwargs):
        def decorate(f):
            return f  # the module-level `pipeline_controller()` call then runs the body once

        return decorate(fn) if fn is not None else decorate

    def module(name, **attrs):
        mod = types.ModuleType(name)
        mod.__dict__.update(attrs)
        sys.modules[name] = mod
        return mod

    module("airflow")
    module("airflow.sdk", dag=dag, task=task)
    module("airflow.providers")
    module("airflow.providers.standard")
    module("airflow.providers.standard.operators")
    module(
        "airflow.providers.standard.operators.trigger_dagrun",
        TriggerDagRunOperator=MagicMock(name="TriggerDagRunOperator"),
    )
    return registry


@pytest.fixture(scope="session")
def controller():
    sys.path.insert(0, str(DAGS_DIR))  # so `from utils import ...` works like it does in Airflow
    if AIRFLOW_INSTALLED:
        module = importlib.import_module(DAG_MODULE)
        dag = module.pipeline_controller()
        tasks = {
            t.task_id: t.python_callable for t in dag.tasks if hasattr(t, "python_callable")
        }
    else:
        registry = _install_airflow_stubs()
        module = importlib.import_module(DAG_MODULE)
        dag, tasks = None, registry
    return SimpleNamespace(module=module, dag=dag, tasks=tasks)


@pytest.fixture
def make_dag_run():
    """Factory for a minimal stand-in of Airflow's DagRun."""

    def _make(run_id="manual__2024-06-01T00:00:00", **conf):
        return SimpleNamespace(conf=conf, run_id=run_id)

    return _make


@pytest.fixture
def dag_mapping(monkeypatch, controller):
    """Replace the DAG_MAPPING the tasks read with known test data."""
    mapping = {
        "jc": {"dag": "ingest_jc", "pipeline_name": "trips_jc", "fallback_date": "2021-01"},
        "nyc": {"dag": "ingest_nyc", "pipeline_name": "trips_nyc", "fallback_date": "2026-01"},
    }
    monkeypatch.setattr(controller.module, "DAG_MAPPING", mapping)
    return mapping