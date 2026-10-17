"""
Fixtures for testing the tasks defined inside the @dag functions of your DAG files.

The tasks are nested in the @dag function, so they can't be imported directly.
`load_dag(module_name, factory_name)` returns an object with:
    .module  the imported DAG module (use it to monkeypatch names the tasks use)
    .tasks   {task_name: plain undecorated python function}
    .dag     the real DAG object (None when Airflow isn't installed)

Two modes, chosen automatically:
  * Airflow installed (e.g. inside your Airflow docker image): the real DAG is built
    and each task's `python_callable` is extracted.
  * Airflow NOT installed: tiny stand-ins for `airflow.sdk` and TriggerDagRunOperator are
    registered in sys.modules, so the DAG files import and the task functions are captured.
"""
import enum
import importlib
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# --- adjust to your project layout ------------------------------------------
DAGS_DIR = Path(__file__).resolve().parents[2] / "airflow" / "dags"  # folder with the DAG files and utils.py
DAG_MODULE = "controller_dag"          # used by the `controller` fixture below
CONTROLLER_FACTORY = "pipeline_controller"
# -----------------------------------------------------------------------------

try:
    import airflow.sdk  # noqa: F401

    AIRFLOW_INSTALLED = True
except ImportError:
    AIRFLOW_INSTALLED = False

# registry that the stub `@task` decorator writes into (swapped for every DAG module loaded)
_STATE = {"stubs_installed": False, "registry": {}}


def _install_airflow_stubs():
    """Register fake airflow modules (once per session)."""
    if _STATE["stubs_installed"]:
        return

    def task(fn=None, **_kwargs):
        def decorate(f):
            _STATE["registry"][f.__name__] = f
            # in a real DAG, calling a task returns an XComArg placeholder, not a result;
            # a MagicMock also supports `>>` chaining, like the real thing
            return MagicMock(name=f"xcomarg:{f.__name__}")

        return decorate(fn) if fn is not None else decorate

    def dag(fn=None, **_kwargs):
        def decorate(f):
            return f  # the module-level `my_dag()` call then runs the body once

        return decorate(fn) if fn is not None else decorate

    class TriggerRule(str, enum.Enum):
        ALL_SUCCESS = "all_success"
        ALL_FAILED = "all_failed"
        ALL_DONE = "all_done"
        ONE_SUCCESS = "one_success"
        ONE_FAILED = "one_failed"
        NONE_FAILED = "none_failed"
        NONE_SKIPPED = "none_skipped"

    def module(name, **attrs):
        mod = types.ModuleType(name)
        mod.__dict__.update(attrs)
        sys.modules[name] = mod
        return mod

    module("airflow")
    module("airflow.sdk", dag=dag, task=task, TriggerRule=TriggerRule)
    module("airflow.providers")
    module("airflow.providers.standard")
    module("airflow.providers.standard.operators")
    module(
        "airflow.providers.standard.operators.trigger_dagrun",
        TriggerDagRunOperator=MagicMock(name="TriggerDagRunOperator"),
    )
    _STATE["stubs_installed"] = True


def _load_dag_module(module_name, factory_name):
    if str(DAGS_DIR) not in sys.path:
        sys.path.insert(0, str(DAGS_DIR))  # so `from utils import ...` works like it does in Airflow

    if AIRFLOW_INSTALLED:
        module = importlib.import_module(module_name)
        dag = getattr(module, factory_name)()
        tasks = {t.task_id: t.python_callable for t in dag.tasks if hasattr(t, "python_callable")}
    else:
        _install_airflow_stubs()
        registry = {}
        _STATE["registry"] = registry
        module = importlib.import_module(module_name)
        dag, tasks = None, registry
    return SimpleNamespace(module=module, dag=dag, tasks=tasks)


@pytest.fixture(scope="session")
def load_dag():
    """Loader for any DAG module: load_dag("my_dag_file", "my_dag_function"). Cached per module."""
    cache = {}

    def _load(module_name, factory_name):
        if module_name not in cache:
            cache[module_name] = _load_dag_module(module_name, factory_name)
        return cache[module_name]

    return _load


@pytest.fixture(scope="session")
def controller(load_dag):
    return load_dag(DAG_MODULE, CONTROLLER_FACTORY)


@pytest.fixture
def make_dag_run():
    """Factory for a minimal stand-in of Airflow's DagRun."""

    def _make(run_id="manual__2024-06-01T00:00:00", **conf):
        return SimpleNamespace(conf=conf, run_id=run_id)

    return _make


@pytest.fixture
def make_ti():
    """Factory for a minimal stand-in of Airflow's TaskInstance."""

    def _make(try_number=1):
        return SimpleNamespace(try_number=try_number)

    return _make


@pytest.fixture
def dag_mapping(monkeypatch, controller):
    """Replace the DAG_MAPPING the controller tasks read with known test data."""
    mapping = {
        "jc": {"dag": "ingest_jc", "pipeline_name": "trips_jc", "fallback_date": "2021-01"},
        "nyc": {"dag": "ingest_nyc", "pipeline_name": "trips_nyc", "fallback_date": "2026-01"},
    }
    monkeypatch.setattr(controller.module, "DAG_MAPPING", mapping)
    return mapping