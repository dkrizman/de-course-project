import pytest


@pytest.fixture
def build_trigger_configs(controller):
    return controller.tasks["build_trigger_configs"]


def test_builds_one_config_per_month(build_trigger_configs, make_dag_run):
    result = build_trigger_configs(
        ["2024-01", "2024-02"], "ingest_jc", dag_run=make_dag_run(run_id="manual__X")
    )
    assert result == [
        {
            "trigger_dag_id": "ingest_jc",
            "trigger_run_id": "manual__X__ingest_jc__2024-01",
            "conf": {"month": "2024-01"},
        },
        {
            "trigger_dag_id": "ingest_jc",
            "trigger_run_id": "manual__X__ingest_jc__2024-02",
            "conf": {"month": "2024-02"},
        },
    ]


def test_empty_months_gives_empty_list(build_trigger_configs, make_dag_run):
    assert build_trigger_configs([], "ingest_jc", dag_run=make_dag_run()) == []


def test_order_is_preserved(build_trigger_configs, make_dag_run):
    months = ["2024-03", "2024-01", "2024-02"]
    result = build_trigger_configs(months, "d", dag_run=make_dag_run())
    assert [c["conf"]["month"] for c in result] == months


def test_trigger_run_ids_are_unique_per_month(build_trigger_configs, make_dag_run):
    result = build_trigger_configs(["2024-01", "2024-02", "2024-03"], "d", dag_run=make_dag_run())
    ids = [c["trigger_run_id"] for c in result]
    assert len(set(ids)) == len(ids)


def test_run_id_differs_between_controller_runs(build_trigger_configs, make_dag_run):
    a = build_trigger_configs(["2024-01"], "d", dag_run=make_dag_run(run_id="run_a"))
    b = build_trigger_configs(["2024-01"], "d", dag_run=make_dag_run(run_id="run_b"))
    assert a[0]["trigger_run_id"] != b[0]["trigger_run_id"]