import pytest


@pytest.fixture
def choose_dag(controller):
    return controller.tasks["choose_dag"]


@pytest.mark.parametrize("market, expected", [("jc", "ingest_jc"), ("nyc", "ingest_nyc")])
def test_returns_dag_for_known_market(choose_dag, dag_mapping, make_dag_run, market, expected):
    assert choose_dag(dag_run=make_dag_run(market=market)) == expected


@pytest.mark.parametrize("market", ["xx", "", "DE"])  # lookup is case-sensitive
def test_unknown_market_raises_value_error(choose_dag, dag_mapping, make_dag_run, market):
    with pytest.raises(ValueError, match=f"Unknown market: {market}"):
        choose_dag(dag_run=make_dag_run(market=market))


def test_missing_market_raises_keyerror(choose_dag, dag_mapping, make_dag_run):
    with pytest.raises(KeyError):
        choose_dag(dag_run=make_dag_run())