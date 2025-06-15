from abc import ABC, abstractmethod


class IndicatorContainer(ABC):

    @abstractmethod
    def find(self, input_: str, asset: str = None):
        pass
