import getpass
import pathlib
import os


def tick_data_home() -> pathlib.Path:
    tick_data_home = os.environ.get("QSIG_TICKDATA_DIR", None)
    if tick_data_home is None:
        return pathlib.Path(f"/home/{getpass.getuser()}/mdhome/tickdata")
    else:
        return pathlib.Path(tick_data_home)


def reports_home() -> pathlib.Path:
    reports_dir = os.environ.get("QSIG_REPORTS_DIR", None)
    if reports_dir is None:
        return pathlib.Path(f"/home/{getpass.getuser()}/qsig_reports")
    else:
        return pathlib.Path(reports_dir)
