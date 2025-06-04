import pandas as pd

from qsig.model.instrument import Instrument
from .indicator_factory import IndicatorFactory, IndicatorContainer
from typing import Union


class IndicatorCache(IndicatorContainer):

    def __init__(self,
                 instrument: Union[Instrument, None]):
        self._indicators = dict()
        self._inst = instrument
        self._data = None
        self._window_sec = 0

    def __repr__(self):

        if isinstance(self._inst, Instrument):
            return f"{self.__class__.__name__}({self._inst.ticker()})"
        elif self._inst is None:
            return f"{self.__class__.__name__})"
        else:
            return f"{self.__class__.__name__}({self._inst}))"

    def add_indicator(self, cls: str, config: dict = None, **kwargs):

        # determine if the expression is an indicator class or an inline
        # indicator expression
        if "(" in cls:
            indicator = IndicatorFactory.instance().create_from_expr(cls, self)
        else:
            if config is None:
                config = kwargs
            else:
                assert len(kwargs) == 0, "cannot provide config and kwargs"

            indicator = IndicatorFactory.instance().create(cls, config, self)

        name = indicator.name
        if name in self._indicators:
            raise Exception("{} cannot add duplicate indicator '{}'".format(
                self.__class__.__name__, name))

        self._indicators[name] = indicator
        print(f"added indicator {indicator} -- {repr(indicator)}")
        return indicator

    def add_data(self, data: pd.DataFrame):
        self._data = data
        self._window_sec = (data.index[1] - data.index[0]).seconds

    def list(self):
        return [x for x in self._indicators.values()]

    def names(self) -> list:
        return sorted([x.name for x in self._indicators.values()])

    def indicators(self):
        return [x for x in self._indicators.values()]

    def interval(self):
        """Periodicity of the underlying data, in seconds"""
        return self._window_sec

    def find(self, source: str):
        indicator = self._indicators.get(source)
        if indicator is not None:
            if self._data is not None and source in self._data.columns:
                raise Exception(f"{self} has ambiguous data and indicator source '{source}'")
            return indicator.result(slot="")
        if self._data is not None and source in self._data.columns:
            return self._data[source]
        raise Exception(f"{self} does not contain data or indicator named '{source}'")


    def compute(self):
        """Compute all indicators"""

        for indicator in self._indicators.values():
            indicator.clear()

        for indicator in self._indicators.values():
            indicator.compute()


    def to_frame(self):
        results = [self._data]
        for indicator in self._indicators.values():
            results.extend(indicator.result_list())
        df = pd.concat(results, axis=1)
        df.sort_index(axis=1, inplace=True)
        return df
