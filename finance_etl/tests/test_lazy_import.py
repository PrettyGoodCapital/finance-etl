import subprocess
import sys


def _run(code: str) -> None:
    subprocess.run([sys.executable, "-c", code], check=True)


def test_locating_config_package_does_not_import_ccflow():
    # lerna resolves pkg://finance_etl.config on every Hydra app startup
    _run("import importlib.resources, sys; importlib.resources.files('finance_etl.config'); assert 'ccflow' not in sys.modules, sorted(sys.modules)")


def test_resolving_one_export_imports_only_its_submodule():
    _run(
        "import sys, finance_etl; finance_etl.SECHTTPModel; assert 'finance_etl.providers.sec' in sys.modules; assert 'finance_etl.providers.massive' not in sys.modules; assert 'finance_etl.calendars' not in sys.modules"
    )


def test_star_import_and_dir_list_every_export():
    _run(
        "import finance_etl; names = set(finance_etl.__all__); assert {'ExchangeCalendar', 'MassiveAllTickersModel', 'SECHTTPModel', 'USFederalCalendar'} <= names; assert names <= set(dir(finance_etl))"
    )
