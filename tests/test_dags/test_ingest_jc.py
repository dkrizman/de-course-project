"""
Tests for the tasks in the ingest_jc DAG.

Mocked: subprocess.run (no docker is ever called), utils.record_success (no DB),
uuid.uuid4 (deterministic container name) and the PROJECT_DIR env var.
"""
import re
import subprocess
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# --- adjust if your file / function names differ ------------------------------
INGEST_MODULE = "ingest_jc_dag"   # file name without .py
INGEST_FACTORY = "ingest_jc_dag"  # name of the @dag-decorated function
# -------------------------------------------------------------------------------

PROJECT_DIR = "/opt/project"
CONTAINER = "pipeline-test-container"


# ---- fixtures -------------------------------------------------------------------
@pytest.fixture
def ingest(load_dag):
    return load_dag(INGEST_MODULE, INGEST_FACTORY)


@pytest.fixture(autouse=True)
def project_dir(monkeypatch):
    monkeypatch.setenv("PROJECT_DIR", PROJECT_DIR)


@pytest.fixture
def mock_run(ingest):
    """Replaces subprocess.run as seen by the DAG module."""
    with patch.object(ingest.module.subprocess, "run") as run:
        yield run


@pytest.fixture
def record_success(monkeypatch, ingest):
    mock = MagicMock(name="record_success")
    monkeypatch.setattr(ingest.module, "record_success", mock)
    return mock


def exec_cmd(layer, window, container=CONTAINER, job="jc"):
    return [
        "docker", "exec",
        "-e", f"LAYER={layer}",
        "-e", f"JOB={job}",
        "-e", f"WINDOW={window}",
        container,
        "python", "-m", "pipeline",
    ]


def window_of(cmd):
    return next(a for a in cmd if a.startswith("WINDOW=")).split("=", 1)[1]


def failing_on(*windows):
    """subprocess.run side_effect that fails (CalledProcessError) for the given WINDOW values."""

    def _side_effect(cmd, **_kwargs):
        if window_of(cmd) in windows:
            raise subprocess.CalledProcessError(1, cmd)

    return _side_effect


# =================================================================================
# days_in_month (module-level helper)
# =================================================================================
@pytest.mark.parametrize(
    "month, expected",
    [
        ("2024-01", 31),
        ("2024-04", 30),
        ("2024-02", 29),  # leap year
        ("2023-02", 28),
        ("1900-02", 28),  # divisible by 100, not a leap year
        ("2000-02", 29),  # divisible by 400, leap year
        ("2024-12", 31),
    ],
)
def test_days_in_month(ingest, month, expected):
    assert ingest.module.days_in_month(month) == expected


@pytest.mark.parametrize("bad", ["2024-13", "2024-00", "2024", "", "abcd-ef", "2024-1-5"])
def test_days_in_month_rejects_invalid_input(ingest, bad):
    with pytest.raises(ValueError):
        ingest.module.days_in_month(bad)


# =================================================================================
# start_container
# =================================================================================
def test_start_container_runs_docker_compose_and_returns_name(ingest, mock_run, monkeypatch):
    monkeypatch.setattr(ingest.module.uuid, "uuid4", lambda: SimpleNamespace(hex="deadbeef"))

    result = ingest.tasks["start_container"]()

    assert result == "pipeline-deadbeef"
    mock_run.assert_called_once_with(
        [
            "docker", "compose",
            "--profile", "ingest",
            "run", "-d", "--build",
            "--name", "pipeline-deadbeef",
            "ingest",
            "sleep", "infinity",
        ],
        cwd=PROJECT_DIR,
        check=True,
        stderr=subprocess.STDOUT,
    )


def test_container_names_are_unique_and_well_formed(ingest, mock_run):
    names = {ingest.tasks["start_container"]() for _ in range(5)}
    assert len(names) == 5
    assert all(re.fullmatch(r"pipeline-[0-9a-f]{32}", n) for n in names)


def test_start_container_propagates_docker_failure(ingest, mock_run):
    mock_run.side_effect = subprocess.CalledProcessError(1, "docker compose")
    with pytest.raises(subprocess.CalledProcessError):
        ingest.tasks["start_container"]()


def test_start_container_requires_project_dir(ingest, mock_run, monkeypatch):
    monkeypatch.delenv("PROJECT_DIR")
    with pytest.raises(KeyError):
        ingest.tasks["start_container"]()
    mock_run.assert_not_called()


# =================================================================================
# run_ingest / run_transform_silver  (same shape: one docker exec, then record)
# =================================================================================
@pytest.fixture(
    params=[("run_ingest", "ingest-to-bronze"), ("run_transform_silver", "transform-to-silver")],
    ids=["ingest-to-bronze", "transform-to-silver"],
)
def layer_task(request, ingest):
    name, layer = request.param
    return SimpleNamespace(fn=ingest.tasks[name], layer=layer)


def test_layer_task_executes_pipeline_in_container(layer_task, mock_run, record_success, make_dag_run, make_ti):
    layer_task.fn(CONTAINER, dag_run=make_dag_run(month="2024-03"), ti=make_ti())

    mock_run.assert_called_once_with(
        exec_cmd(layer_task.layer, "2024-03"),
        cwd=PROJECT_DIR,
        check=True,
        stderr=subprocess.STDOUT,
    )


@pytest.mark.parametrize("try_number", [1, 2, 4])
def test_layer_task_records_success_with_try_number(
    layer_task, mock_run, record_success, make_dag_run, make_ti, try_number
):
    layer_task.fn(CONTAINER, dag_run=make_dag_run(month="2024-03"), ti=make_ti(try_number))

    record_success.assert_called_once_with("jc", "2024-03", layer_task.layer, try_number, "success")


def test_layer_task_records_failure_and_reraises(layer_task, mock_run, record_success, make_dag_run, make_ti):
    mock_run.side_effect = subprocess.CalledProcessError(1, "docker exec")

    with pytest.raises(subprocess.CalledProcessError):
        layer_task.fn(CONTAINER, dag_run=make_dag_run(month="2024-03"), ti=make_ti(2))

    record_success.assert_called_once_with("jc", "2024-03", layer_task.layer, 2, "failed")


def test_layer_task_missing_month_raises_before_running(layer_task, mock_run, record_success, make_dag_run, make_ti):
    with pytest.raises(KeyError):
        layer_task.fn(CONTAINER, dag_run=make_dag_run(), ti=make_ti())
    mock_run.assert_not_called()
    record_success.assert_not_called()


def test_layer_task_missing_project_dir_is_not_recorded_as_failure(
    layer_task, mock_run, record_success, make_dag_run, make_ti, monkeypatch
):
    # only CalledProcessError is handled; a config error just fails the task
    monkeypatch.delenv("PROJECT_DIR")
    with pytest.raises(KeyError):
        layer_task.fn(CONTAINER, dag_run=make_dag_run(month="2024-03"), ti=make_ti())
    record_success.assert_not_called()


def test_layer_task_non_process_error_is_not_recorded_as_failure(
    layer_task, mock_run, record_success, make_dag_run, make_ti
):
    # e.g. docker binary missing -> task fails but no "failed" row is written
    mock_run.side_effect = FileNotFoundError("docker")
    with pytest.raises(FileNotFoundError):
        layer_task.fn(CONTAINER, dag_run=make_dag_run(month="2024-03"), ti=make_ti())
    record_success.assert_not_called()


def test_layer_task_record_success_error_is_not_masked_as_failed(
    layer_task, mock_run, record_success, make_dag_run, make_ti
):
    record_success.side_effect = RuntimeError("db down")
    with pytest.raises(RuntimeError, match="db down"):
        layer_task.fn(CONTAINER, dag_run=make_dag_run(month="2024-03"), ti=make_ti())
    record_success.assert_called_once()  # only the "success" attempt, no follow-up "failed"


# =================================================================================
# run_transform_gold  (one docker exec per day of the month)
# =================================================================================
@pytest.fixture
def run_gold(ingest):
    return ingest.tasks["run_transform_gold"]


@pytest.mark.parametrize(
    "month, days",
    [("2024-01", 31), ("2024-04", 30), ("2024-02", 29), ("2023-02", 28)],
)
def test_gold_runs_one_exec_per_day_in_order(run_gold, mock_run, record_success, make_dag_run, make_ti, month, days):
    run_gold(CONTAINER, dag_run=make_dag_run(month=month), ti=make_ti())

    windows = [window_of(c.args[0]) for c in mock_run.call_args_list]
    assert windows == [f"{month}-{d:02d}" for d in range(1, days + 1)]


def test_gold_command_and_options(run_gold, mock_run, record_success, make_dag_run, make_ti):
    run_gold(CONTAINER, dag_run=make_dag_run(month="2024-02"), ti=make_ti())

    first, last = mock_run.call_args_list[0], mock_run.call_args_list[-1]
    assert first.args[0] == exec_cmd("transform-to-gold", "2024-02-01")
    assert last.args[0] == exec_cmd("transform-to-gold", "2024-02-29")
    for call in mock_run.call_args_list:
        assert call.kwargs == {"cwd": PROJECT_DIR, "check": True, "stderr": subprocess.STDOUT}


def test_gold_all_days_succeed_records_success(run_gold, mock_run, record_success, make_dag_run, make_ti):
    run_gold(CONTAINER, dag_run=make_dag_run(month="2024-01"), ti=make_ti(3))

    record_success.assert_called_once_with("jc", "2024-01", "transform-to-gold", 3, "success", 31, [])


def test_gold_continues_after_failed_days_and_records_them(run_gold, mock_run, record_success, make_dag_run, make_ti):
    mock_run.side_effect = failing_on("2024-01-03", "2024-01-10")

    with pytest.raises(Exception):  # the task must fail (see xfail test below about the error type)
        run_gold(CONTAINER, dag_run=make_dag_run(month="2024-01"), ti=make_ti(2))

    assert mock_run.call_count == 31  # a failing day does not stop the loop
    record_success.assert_called_once_with(
        "jc", "2024-01", "transform-to-gold", 2, "failed", 31, ["2024-01-03", "2024-01-10"]
    )


def test_gold_all_days_failing(run_gold, mock_run, record_success, make_dag_run, make_ti):
    mock_run.side_effect = subprocess.CalledProcessError(1, "docker exec")

    with pytest.raises(Exception):
        run_gold(CONTAINER, dag_run=make_dag_run(month="2023-02"), ti=make_ti())

    expected_days = [f"2023-02-{d:02d}" for d in range(1, 29)]
    record_success.assert_called_once_with("jc", "2023-02", "transform-to-gold", 1, "failed", 28, expected_days)


def test_gold_last_day_failure_is_detected(run_gold, mock_run, record_success, make_dag_run, make_ti):
    mock_run.side_effect = failing_on("2024-04-30")

    with pytest.raises(Exception):
        run_gold(CONTAINER, dag_run=make_dag_run(month="2024-04"), ti=make_ti())

    assert record_success.call_args.args[-1] == ["2024-04-30"]


@pytest.mark.parametrize("bad_month", ["2024-13", "nonsense", ""])
def test_gold_invalid_month_fails_before_running_anything(
    run_gold, mock_run, record_success, make_dag_run, make_ti, bad_month
):
    with pytest.raises(ValueError):
        run_gold(CONTAINER, dag_run=make_dag_run(month=bad_month), ti=make_ti())
    mock_run.assert_not_called()
    record_success.assert_not_called()


def test_gold_missing_month_raises_keyerror(run_gold, mock_run, record_success, make_dag_run, make_ti):
    with pytest.raises(KeyError):
        run_gold(CONTAINER, dag_run=make_dag_run(), ti=make_ti())
    mock_run.assert_not_called()


def test_gold_non_process_error_stops_immediately_without_record(
    run_gold, mock_run, record_success, make_dag_run, make_ti
):
    mock_run.side_effect = FileNotFoundError("docker")
    with pytest.raises(FileNotFoundError):
        run_gold(CONTAINER, dag_run=make_dag_run(month="2024-01"), ti=make_ti())
    assert mock_run.call_count == 1
    record_success.assert_not_called()


def test_gold_failure_error_names_the_failed_days(run_gold, mock_run, record_success, make_dag_run, make_ti):
    mock_run.side_effect = failing_on("2024-01-03")
    with pytest.raises(Exception, match="2024-01-03"):
        run_gold(CONTAINER, dag_run=make_dag_run(month="2024-01"), ti=make_ti())


# =================================================================================
# cleanup_container
# =================================================================================
def test_cleanup_force_removes_container_without_raising_on_failure(ingest, mock_run):
    result = ingest.tasks["cleanup_container"](CONTAINER)

    assert result is None
    mock_run.assert_called_once_with(
        ["docker", "rm", "-f", CONTAINER],
        check=False,  # a missing container must not fail the cleanup
        stderr=subprocess.STDOUT,
    )


# =================================================================================
# DAG structure (real DAG object; needs Airflow installed, e.g. in your docker image)
# =================================================================================
# @pytest.fixture
# def real_dag(ingest):
#     """The real DAG object; these tests are skipped when Airflow isn't installed."""
#     if ingest.dag is None:
#         pytest.skip("requires apache-airflow")
#     return ingest.dag


# def test_dag_settings(real_dag):
#     assert real_dag.dag_id == "ingest_jc"
#     assert real_dag.catchup is False
#     assert "bronze" in real_dag.tags


# def test_expected_tasks_exist(real_dag):
#     assert set(real_dag.task_dict) == {
#         "start_container",
#         "run_ingest",
#         "run_transform_silver",
#         "run_transform_gold",
#         "cleanup_container",
#     }


# def test_layers_run_in_order_after_container_starts(real_dag):
#     get = real_dag.get_task
#     assert get("run_ingest").upstream_task_ids == {"start_container"}
#     assert get("run_transform_silver").upstream_task_ids == {"start_container", "run_ingest"}
#     assert get("run_transform_gold").upstream_task_ids == {"start_container", "run_transform_silver"}


# def test_cleanup_waits_for_everything_and_runs_even_on_failure(real_dag):
#     cleanup = real_dag.get_task("cleanup_container")
#     assert cleanup.upstream_task_ids == {
#         "start_container",
#         "run_ingest",
#         "run_transform_silver",
#         "run_transform_gold",
#     }
#     assert cleanup.trigger_rule == "all_done"


# @pytest.mark.parametrize("task_id", ["run_ingest", "run_transform_silver", "run_transform_gold"])
# def test_layer_tasks_retry_three_times_with_five_second_delay(real_dag, task_id):
#     task = real_dag.get_task(task_id)
#     assert task.retries == 3
#     assert task.retry_delay == timedelta(seconds=5)