import pandas as pd
import math

from qsig import DataRepo, DataRepoError


def _test_write_read(repo: DataRepo, key="default"):

    library_name = "test_write_read"
    lib = repo.get_library(library_name)

    data = pd.DataFrame({'a': [1, 2, 3], 'b': [10, 22, math.nan]})
    lib.write(key, data)

    keys = lib.list_keys()
    assert key in keys
    recovered_data = lib.read(key)

    assert data.fillna(0).eq(recovered_data.fillna(0)).all().all()


    lib.delete(key)
    keys = lib.list_keys()
    assert key not in keys

    try:
        lib.read(key)
    except DataRepoError as e:
        assert e.key == key


def _test_read_write_names(repo):
    library_name = "test_read_write_names"
    lib = repo.get_library(library_name)

    keys = {"close_prices", "BTC/USD", "BTC/USD@close", "BTC/USD@close", "BBG:RR/ LN EQUITY"}

    for key in keys:
        lib.write(key, pd.DataFrame())

    inventory = set(lib.list_keys())
    assert inventory == keys

    try:
        lib.write("", pd.DataFrame())
        assert False
    except DataRepoError:
        pass


def _test_key_with_period_ext(repo):
    library_name = "test_key_with_period_ext"

    repo.delete_library(library_name)
    lib = repo.get_library(library_name)

    lib.write("_part.open.BTC/USDT.KUC", pd.DataFrame())
    assert set(lib.list_keys()) == {"_part.open.BTC/USDT.KUC"}

    lib.write("_part.open.BTC/USDT.HTX", pd.DataFrame())
    assert set(lib.list_keys()) == {"_part.open.BTC/USDT.KUC", "_part.open.BTC/USDT.HTX"}


# ----------------------------------------------------------------------

def test_read_writes():
    repo = DataRepo(storage_path="/var/tmp/DATAREPO_TEST")
    _test_write_read(repo, key="close_prices")
    _test_write_read(repo, key="_part.open.BTC/USDT.KUC")
    _test_write_read(repo, key="BTC/USD")
    _test_write_read(repo, key="BTC/USD@close")
    _test_write_read(repo, key="BTC/USD@close")
    _test_write_read(repo, key="BBG:RR/ LN EQUITY")


def test_read_write_names():
    repo = DataRepo(storage_path="/var/tmp/DATAREPO_TEST")
    _test_read_write_names(repo)


def test_key_with_period_ext():
    repo = DataRepo(storage_path="/var/tmp/DATAREPO_TEST")
    _test_key_with_period_ext(repo)
