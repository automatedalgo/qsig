import talib
from dataclasses import dataclass
import pandas as pd

from qsig.util.signal import halflife_to_span, data_interval
from .indicator_factory import IndicatorFactory
import qsig.util.time


# TA-Lib EMA Indicator details (for GenericIndicator)
class EMA:

    @dataclass
    class Params:
        halflife: int

    @staticmethod
    def calc(owner, params: Params, data: pd.Series):
        span = round(halflife_to_span(params.halflife, data_interval(data)))
        return talib.EMA(data, span)

    # Parse a config object and returns the Params and indicator input
    @staticmethod
    def parse_config(config: dict):
        halflife = qsig.util.time.parse_time_period(config["halflife"])
        input_ = config.get("input")
        return EMA.Params(halflife=halflife), input_

    # Parse the parts of an inline indicator definition, and return
    # corresponding configuration objects (to use with parse_config)
    @staticmethod
    def params_to_config(params: list, inputs: list):
        assert len(params) == 1
        return {"halflife": params[0]}, {"input": inputs[0]}


IndicatorFactory.instance().register("EMA", EMA)


# TA-Lib RSI Indicator details (for GenericIndicator).
#
# Input is a timeperiod integer, referring to bin interval.
#
class RSI:

    @dataclass
    class Params:
        timeperiod: int

    @staticmethod
    def calc(_, params: Params, data: pd.Series):
        return talib.RSI(data, params.timeperiod)

    # Parse a config object and returns the Params and indicator input
    @staticmethod
    def parse_config(config: dict):
        timeperiod = config["timeperiod"]
        input_ = config.get("input")
        return RSI.Params(timeperiod), input_

    # Parse the parts of an inline indicator definition, and return
    # corresponding configuration objects (to use with parse_config)
    @staticmethod
    def params_to_config(params: list, inputs: list):
        assert params is not None
        assert len(params) == 1
        return {"timeperiod": int(params[0])}, {"input": inputs[0]}


IndicatorFactory.instance().register("RSI", RSI)
