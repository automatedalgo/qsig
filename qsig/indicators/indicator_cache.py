import pandas as pd

from qsig.model.instrument import Instrument
from .indicator_factory import IndicatorFactory
from .indicator_container import IndicatorContainer
from typing import Union


class ItemIndicatorCache(IndicatorContainer):
    """Container for indicators related to a single tradable item, such as an
    asset or index."""
    def __init__(self,
                 instrument: Union[Instrument, None, str],
                 parent: IndicatorContainer = None):
        self._indicators = dict()
        self._inst = instrument
        self._data = None
        self._parent = parent

    def __repr__(self):

        if isinstance(self._inst, Instrument):
            return f"{self.__class__.__name__}({self._inst.ticker()})"
        elif self._inst is None:
            return f"{self.__class__.__name__})"
        else:
            return f"{self.__class__.__name__}({self._inst}))"

    def ticker(self):
        if isinstance(self._inst, Instrument):
            return self._inst.ticker()
        else:
            return self._inst or ""

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
        print(f"added indicator '{indicator}' = {repr(indicator)} on {self.ticker()}")
        return indicator

    def add_data(self, data: pd.DataFrame):
        self._data = data

    def list(self):
        return [x for x in self._indicators.values()]

    def names(self) -> list:
        return sorted([x.name for x in self._indicators.values()])

    def has_name(self, name: str):
        return name in self.names()

    def indicators(self):
        return [x for x in self._indicators.values()]

    def find(self, source: str, asset: str = None):
        indicator = self._indicators.get(source)
        if indicator is not None:
            if self._data is not None and source in self._data.columns:
                raise Exception(f"{self} has ambiguous data and indicator source '{source}'")
            return indicator.result(slot="")
        if self._data is not None and source in self._data.columns:
            return self._data[source]
        if self._parent:
            return self._parent.find(source, self.ticker())
        raise Exception(f"{self} does not contain data or indicator named '{source}'")

    def compute(self):
        """Compute all indicators"""

        for indicator in self._indicators.values():
            indicator.clear()

        for indicator in self._indicators.values():
            indicator.compute()

    def to_frame(self, skip_non_computed=False):
        results = [self._data]
        for indicator in self._indicators.values():
            if indicator.is_computed():
                results.extend(indicator.result_list())
            elif not skip_non_computed:
                raise Exception(f"indicator not yet computed, for '{indicator}'")
        df = pd.concat(results, axis=1)
        df.sort_index(axis=1, inplace=True)
        return df
