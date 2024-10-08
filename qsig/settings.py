import getpass
import pathlib
import os


def tick_data_home() -> pathlib.Path:
    return pathlib.Path(f"/home/{getpass.getuser()}/apex/data/tickdata")


def reports_home() -> pathlib.Path:
    reports_dir = os.environ.get("QSIG_REPORTS_DIR", None)
    if reports_dir is None:
        return pathlib.Path(f"/home/{getpass.getuser()}/qsig_reports")
    else:
        return pathlib.Path(reports_dir)
