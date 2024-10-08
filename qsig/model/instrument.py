from enum import Enum
from dataclasses import dataclass


class ExchCode(str, Enum):
    BINANCE = "binance"
    KUCOIN = "KCN"
    HTX = "HTX"


@dataclass
class ExchangeInfo:
    name: str
    slug: str
    short_code: str


# Either load from file, or, have in code
Exchange_Map = {
    ExchCode.BINANCE: ExchangeInfo("Binance", "binance", "BNC"),
    ExchCode.KUCOIN: ExchangeInfo("KuCoin", "kucoin", "KUC"),
    ExchCode.HTX: ExchangeInfo("HTX", "htc", "HTX")
}


# An instrument represents a tradable security located at a specific venue
@dataclass
class Instrument:
    base: str
    quote: str
    exch: ExchCode

    @staticmethod
    def from_ticker(ticker, exch):
        base, quote = ticker.split("/")
        return Instrument(base=base, quote=quote, exch=exch)

    def ticker(self):
        return "{}/{}.{}".format(self.base, self.quote, Exchange_Map[self.exch].short_code)

    def __repr__(self):
        return "Instrument({})".format(self.ticker())
