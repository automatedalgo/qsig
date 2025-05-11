import datetime
import logging

from qsig.data.binance.binance_data import build_binance_trade_features_dataset
from qsig.data.binance.binance_fetch_bars import fetch_binance_bars

import qsig


def main():
    qsig.init()

    # --------------------------------------------------------------------------
    # Configuration
    # --------------------------------------------------------------------------

    # date range for core market data
    date_from = datetime.date(2024, 7, 1)
    date_upto = datetime.date(2024, 9, 1)

    # bar intervals we are interested in
    bin_interval = qsig.BarInterval("1h")

    # build the universe, using normalised Instruments
    tickers = ["BTC/USDT", "SOL/USDT", "ETH/USDT", "XRP/USDT", "DOGE/USDT"]
    universe = [qsig.Instrument.from_ticker(t, qsig.ExchCode.BINANCE) for t in tickers]

    # repository to store the core pricing dataset
    repo = qsig.DataRepo("/var/tmp/MY_DATA_REPO")

    # --------------------------------------------------------------------------
    # Fetch the raw market data (OHLC bars) from Binance. These files are stored
    # under the tick data directory.  Files already downloaded are not
    # downloaded again.
    # --------------------------------------------------------------------------

    fetch_binance_bars(universe, date_from, date_upto, bin_interval)

    # --------------------------------------------------------------------------
    # Build core market data features dataframes.  This step will always rebuild
    # the dataset.
    # --------------------------------------------------------------------------

    # create a dedicate data-library in which to store our trade features
    # dataset, with the name specific to the data vendor and bar interval
    lib = repo.get_library(name=f"core_data_{bin_interval}_binance")

    # build the feature dataframes from the Binance raw bars
    build_binance_trade_features_dataset(universe,
                                         date_from,
                                         date_upto,
                                         bin_interval,
                                         lib)

    # --------------------------------------------------------------------------
    # Repo explorer
    # --------------------------------------------------------------------------

    # We have earlier placed the binance data into our DataRepo object.  Here we
    # just do a basic exploration of that libraries and keys exist, just to
    # demonstrate some basic usage concepts.

    logging.info(f"DataRepo library: {lib.name}")
    logging.info(f"DataRepo keys: {lib.list_keys()}")

    # --------------------------------------------------------------------------
    # Plot
    # --------------------------------------------------------------------------

    # Load the close prices data item, and the graph the relative prices using a
    # HTML based plot.
    close = lib.read("close")
    for col in close.columns:
        close[col] = close[col] / close[col].iloc[0]
    qsig.quick_plot(close)

if __name__ == "__main__":
    main()
