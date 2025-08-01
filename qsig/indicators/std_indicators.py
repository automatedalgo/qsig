from typing import Union

from .base_indicators import UnaryIndicator
from .indicator_factory import IndicatorContainer, IndicatorFactory
import qsig
from qsig.util.time import parse_time_period
from qsig.util.signal import calc_density, calc_fwd_returns, data_interval


class SMA(UnaryIndicator):
    """Simple moving average"""

    CODE = "SMA"

    def __init__(self, owner, window: Union[int, str], input_col: str, name: str = None):
        params = [window]
        super().__init__(self.CODE, owner, params, input_col, name=name)
        self.window_sec = qsig.util.time.parse_time_period(window)


    def _compute(self):
        data = self._owner.find(self.source)
        periods = int(self.window_sec / data_interval(data))
        assert (periods * data_interval(data)) == self.window_sec
        result = data.rolling(periods).mean()
        self._store_result(result)


    @classmethod
    def create(cls, args: dict, owner: IndicatorContainer,
               name=None, params=None, sources=None):
        # TODO: need safe config unpacking here
        if args is not None:
            window = args["window"]
            source = args.get("source")
            name = args.get("name")
        else:
            assert len(params) == 1
            window = params[0]
            source = cls._single_source(sources)
        return cls(owner, window, source, name)


IndicatorFactory.instance().register(SMA)


class DEN(UnaryIndicator):
    CODE = "DEN"

    def __init__(self, owner, window: Union[int, str], input_col: str, name: str = None):
        params = [window]
        super().__init__(self.CODE, owner, params, input_col, name=name)
        self.window_sec = qsig.util.time.parse_time_period(window)


    def _compute(self):
        data = self._owner.find(self.source)
        result = calc_density(data=data,
                              data_period_sec=data_interval(data),
                              study_period_sec=self.window_sec)
        self._store_result(result)


    @classmethod
    def create(cls, args: dict, owner: IndicatorContainer,
               name=None, params=None, sources=None):
        # TODO: need safe args unpacking here
        if args is not None:
            window = args["window"]
            source = args["source"]  # want mandatory source
            name = args.get("name")
        else:
            window = params[0]
            source = cls._single_source(sources)
        return cls(owner, window, source, name=name)


IndicatorFactory.instance().register(DEN)


class RET(UnaryIndicator):
    """Return"""

    CODE = "RET"

    def __init__(self, owner, window: Union[int, str], input_col: str = None, name: str = None):
        params = [window]
        super().__init__(self.CODE, owner, params, input_col, name=name)
        self.window_sec = qsig.util.time.parse_time_period(window)

    def _compute(self):
        data = self._owner.find(self.source)
        periods = int(self.window_sec / data_interval(data))
        assert (periods * data_interval(data)) == self.window_sec
        result = data.pct_change(periods=periods, fill_method=None)
        self._store_result(result)

    @classmethod
    def create(cls, args: dict, owner: IndicatorContainer,
               name=None, params=None, sources=None):
        # TODO: need safe args unpacking here
        if args is not None:
            window = args["window"]
            source = args.get("source")
            name = args.get("name")
        else:
            window = params[0]
            source = cls._single_source(sources)

        return cls(owner, window, source, name=name)


IndicatorFactory.instance().register(RET)


# Note: use this only for plotting!  Don't include it in any signal formula
# because it is obviously forward-looking.
class FWD(UnaryIndicator):
    """Forward-looking return"""

    CODE = "FWD"

    def __init__(self, owner, window: Union[int, str], input_col: str = None, name=None):
        params = [window]
        super().__init__(self.CODE, owner, params, input_col, name=name)
        self.window_sec = qsig.util.time.parse_time_period(window)

    def _compute(self):
        data = self._owner.find(self.source)
        result = calc_fwd_returns(data,
                                  data_interval(data),
                                  self.window_sec)
        self._store_result(result)

    @classmethod
    def create(cls,
               args: dict,
               owner: IndicatorContainer,
               name=None, params=None, sources=None):
        # TODO: need safe args unpacking here
        if args is not None:
            window = args["window"]
            source = args.get("source")
            name = args.get("name")
        else:
            window = params[0]
            source = cls._single_source(sources)

        return cls(owner, window, source, name=name)


IndicatorFactory.instance().register(FWD)


class EWMA(UnaryIndicator):
    """Exponentially weighted moving average"""

    CODE = "EWMA"

    def __init__(self, owner, halflife: Union[int, float, str], input_col: str = None,
                 name: str = None):
        params = [halflife]
        super().__init__(self.CODE, owner, params, input_col, name)
        self.halflife = qsig.util.time.parse_time_period(halflife)

    def _compute(self):
        data = self._owner.find(self.source)
        halflife = self.halflife/data_interval(data)
        result = data.ewm(halflife=halflife, adjust=False).mean()
        self._store_result(result)

    @classmethod
    def create(cls,
               args: dict,
               owner: IndicatorContainer,
               name=None, params=None, sources=None):
        # TODO: need safe args unpacking here
        if args is not None:
            halflife = args["halflife"]
            source = args.get("source")
            name = args.get("name")
        else:
            halflife = params[0]
            source = cls._single_source(sources)
        return cls(owner, halflife, source, name=name)


IndicatorFactory.instance().register(EWMA)
