from abc import ABC, abstractmethod


class IndicatorContainer(ABC):

    @abstractmethod
    def find(self, source: str):
        pass

    @abstractmethod
    def interval(self):
        pass
