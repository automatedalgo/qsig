from enum import Enum
from typing import Union

import pandas as pd


class TimeUnit(str, Enum):
    SECOND = "s"
    MINUTE = "m"
    HOUR = "h"

    @staticmethod
    def parse(s: str):
        if s == "s":
            return TimeUnit.SECOND
        elif s == "m":
            return TimeUnit.MINUTE
        elif s == "h":
            return TimeUnit.HOUR
        else:
            raise ValueError(f"time unit string not handled, '{str}'")


class BarInterval:

    count: int
    unit: TimeUnit

    def __init__(self,
                 text: Union[str, None],
                 count: int = None,
                 unit: TimeUnit = None):

        if text is None:
            assert count is not None
            assert unit is not None
            self._count = count
            self._unit = unit
        else:
            assert len(text) > 1
            assert count is None
            assert unit is None
            self._unit = TimeUnit.parse(text[-1])
            self._count = int(text[0:-1])


    @property
    def count(self):
        return self._count

    @property
    def unit(self) -> TimeUnit:
        return self._unit


    def __repr__(self):
        return f"BarInterval({self.count}{str(self.unit.value)})"

    def __str__(self):
        return f"{self.count}{str(self.unit.value)}"


    def to_pandas_timedelta(self):
        if self.unit == TimeUnit.SECOND:
            return pd.Timedelta(seconds=self.count)
        elif self.unit == TimeUnit.MINUTE:
            return pd.Timedelta(minutes=self.count)
        elif self.unit == TimeUnit.HOUR:
            return pd.Timedelta(hours=self.count)
        else:
            raise ValueError("TimeUnit not supported")


    def to_pandas_resample_rule(self):
        if self.unit == TimeUnit.SECOND:
            return f"{self.count}s"
        elif self.unit == TimeUnit.MINUTE:
            return f"{self.count}min"
        elif self.unit == TimeUnit.HOUR:
            return f"{self.count}h"
        else:
            raise ValueError("TimeUnit not supported")


    @staticmethod
    def parse(interval: str):
        assert len(interval) > 1
        unit = TimeUnit.parse(interval[-1])
        count = int(interval[0:-1])
        return BarInterval(text=None, count=count, unit=unit)



BAR_1_SEC = BarInterval(None, 1, TimeUnit.SECOND)
BAR_5_SEC = BarInterval(None, 5, TimeUnit.SECOND)
BAR_15_SEC = BarInterval(None, 15, TimeUnit.SECOND)
BAR_30_SEC = BarInterval(None, 30, TimeUnit.SECOND)
BAR_1_MIN = BarInterval(None, 1, TimeUnit.MINUTE)
BAR_3_MIN = BarInterval(None, 3, TimeUnit.MINUTE)
BAR_5_MIN = BarInterval(None, 5, TimeUnit.MINUTE)
BAR_15_MIN = BarInterval(None, 15, TimeUnit.MINUTE)
BAR_30_MIN = BarInterval(None, 30, TimeUnit.MINUTE)
BAR_1_HOUR = BarInterval(None, 1, TimeUnit.HOUR)
BAR_2_HOUR = BarInterval(None, 1, TimeUnit.HOUR)
BAR_4_HOUR = BarInterval(None, 1, TimeUnit.HOUR)
