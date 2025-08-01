import pandas as pd
import logging
from dataclasses import dataclass

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

    def symbol(self):
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
        logging.debug(f"added indicator '{indicator}' = {repr(indicator)} on {self.symbol()}")
        return indicator

    def add_data(self, data: pd.DataFrame):
        self._data = data

    def list(self):
        return [x for x in self._indicators.values()]

    def indicator_names(self) -> list:
        return sorted([x.name for x in self._indicators.values()])

    def has_indicator(self, name: str):
        return name in self.indicator_names()

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
            return self._parent.find(source, self.symbol())
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



@dataclass
class IndicatorPath:
    symbol: str
    name: str

    def __str__(self):
        return f"{self.symbol}:{self.name}"


# Root level indicator cache.  This will contain individual ItemIndicatorCache
# instances per asset or tradable item.  It can also contain common data sets
# that arranged on a per-feature basis.
class RootIndicatorCache(IndicatorContainer):

    def __init__(self):
        self._data = dict()
        self._item_indicators = None
        self._universe = []

    def universe(self) -> list[str]:
        return self._universe

    def set_universe(self, ids: list[str]):
        if isinstance(ids, str):
            ids = [ ids]
        self._universe = [x for x in ids]

    def find(self, input_: str, asset: str = None):
        if input_ in self._data:
            df = self._data[input_]
            if asset in df.columns:
                return df[asset]

        raise Exception(f"root indicator cache does not contain input '{input_}' for item '{asset}'")

    def add_data(self, data: pd.DataFrame, name: str = None):
        label = name or data.name
        assert label, "data added to indicator must have a name"
        self._data[label] = data

    def _generate_auto_universe(self):
        logging.info("auto generating universe for indicator cache")
        universe = None
        for data in self._data.values():
            if universe is None:
                universe = set([x for x in data.columns])
            else:
                universe = universe.intersection(data.columns)

        self._universe = list(universe)

    def add_indicator(self, cls: str):
        if not self._universe:
            self._generate_auto_universe()
        if self._item_indicators is None:
            self._item_indicators = {x: ItemIndicatorCache(x, self) for x in self._universe}
        for indicator in self._item_indicators.values():
            indicator.add_indicator(cls)

    def compute(self):
        for indicator in self._item_indicators.values():
            indicator.compute()
        return self


    def list_indicators(self):
        names = []
        for symbol, cache in self._item_indicators.items():
            for ind_name in cache.indicator_names():
                names.append(IndicatorPath(symbol, ind_name))
        return names


    def results(self, indicator_name: str = None, symbol: str = None,
                uniform_labels=False):

        def _column_name_indicator(ind_name, _):
            return ind_name

        def _column_name_symbol(_, _ind_symbol):
            return _ind_symbol

        def _column_name_uniform(ind_name, ind_symbol):
            return f"{ind_symbol}:{ind_name}"

        column_namer = _column_name_uniform  # default approach
        if not uniform_labels:
            if indicator_name and not symbol:
                column_namer = _column_name_symbol
            if not indicator_name and symbol:
                column_namer = _column_name_indicator

        dataframes = []
        for symbol_, cache in self._item_indicators.items():
            if symbol is None or symbol_ == symbol:
                for indicator in cache.indicators():
                    if indicator_name is None or indicator_name == indicator.name:
                        result = indicator.result().copy()
                        result.name = column_namer(indicator.name, cache.symbol())
                        dataframes.append(result)
        final = pd.concat(dataframes, axis=1) if dataframes else pd.DataFrame()

        # add a name to the dataframe
        if indicator_name and not symbol:
            final.name = indicator_name
        elif not indicator_name and symbol:
            final.name = symbol
        else:
            # for a fully specified result, we will return as a Series
            if len(dataframes) == 1:
                final = dataframes[0]
            final.name = f"{symbol}:{indicator_name}"

        return final

    def get_item_indicator_cache(self, symbol: str):
        return self._item_indicators[symbol]
