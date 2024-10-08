import logging
import os
import sys

from .model.marketdata import BarInterval
from .model.instrument import ExchCode, Instrument

from .util.report import quick_plot
from .util.datarepo import DataRepo, DataRepoError

__version__ = "0.1.0"

def init(debug=False):
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(fmt)
    logging.getLogger().addHandler(handler)
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

__all__ = [
    "BarInterval",
    "ExchCode",
    "Instrument",
    "log_init",
    "quick_plot",
    "DataRepo",
    "DataRepoError",
    "init"
    ]
