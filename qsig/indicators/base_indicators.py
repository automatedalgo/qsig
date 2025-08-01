from abc import ABC, abstractmethod
from enum import Enum, auto
import pandas as pd

from .indicator_container import IndicatorContainer


class BaseIndicator(ABC):
    """Base class for all indicators. Each indicator will have a unique 'name',
       which will identify it within its parent IndicatorContainer."""

    class _State(Enum):
        CLEAR = auto()
        COMPUTING = auto()
        COMPUTED = auto()

    def __init__(self, code: str, owner: IndicatorContainer, indicator_name: str):
        assert code is not None
        assert '(' not in code
        self._owner = owner
        self._name = indicator_name
        self._results = dict()
        self._is_evaluating = False
        self._compute_state = BaseIndicator._State.CLEAR

    def compute(self):
        if self.is_computed():
            return
        if self._compute_state == BaseIndicator._State.COMPUTING:
            raise Exception(f"circular dependency detected while computing '{self}'")
        self._compute_state = BaseIndicator._State.COMPUTING
        self._compute()
        self._compute_state = BaseIndicator._State.COMPUTED

    @abstractmethod
    def _compute(self):
        pass

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return "{}".format(self.__class__.__name__)

    def __str__(self):
        return self._name

    def clear(self):
        self._results = dict()
        self._compute_state = BaseIndicator._State.CLEAR

    def result(self, slot=""):
        data = self._results.get(slot)
        if data is None:
            self.compute()  # data not ready, so compute on demand
            data = self._results.get(slot)
        return data

    def _store_result(self, data: pd.Series, slot=""):
        data.name = f"{self._name}.{slot}" if slot else f"{self._name}"
        self._results[slot] = data

    def to_frame(self):
        frame = pd.concat([x for x in self._results.values()], axis=1)
        return frame

    def result_list(self):
        return [x for x in self._results.values()]

    def is_computed(self):
        return self._compute_state == BaseIndicator._State.COMPUTED


class UnaryIndicator(BaseIndicator, ABC):
    """A unitary indicator is one that depends on a single input source.
    """

    # Default input to take if not provided
    DEFAULT_SOURCE = "close"

    def __init__(self, code: str, owner: IndicatorContainer, params: list,
                 source: str, name: str = None):

        # build indicator name to be more concise, usable as an ID
        if name is None:
            name = "{}({})".format(code, ",".join(params))
            if source:
                name = f"{name}[{source}]"
        else:
            if name == "":
                raise Exception("indicator name cannot be empty string")

        # build a repr string the retains the class nane
        _repr = "{}(".format(self.__class__.__name__)
        if isinstance(params, list):
            _repr += ",".join([str(x) for x in params])
        else:
            _repr += params

        _repr += f")[{source or self.__class__.DEFAULT_SOURCE}]"
        _repr += f"'{name}'"
        self._repr = _repr

        super().__init__(code, owner, name)
        self.source = source or self.__class__.DEFAULT_SOURCE

    def __repr__(self):
        return self._repr

    @staticmethod
    def _single_source(sources):
        if sources:
            assert len(sources) == 1
            return sources[0]
        else:
            return None
