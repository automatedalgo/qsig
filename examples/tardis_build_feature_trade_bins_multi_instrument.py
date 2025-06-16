import datetime as dt
import pandas as pd
import getpass
import logging

from qsig import BarInterval
from qsig.data.tardis.tardis_downloader import TardisDownloader
from qsig.data.tardis.tardis_binner import create_trade_bins, build_trade_bin_uri
from qsig.model.instrument import Instrument, ExchCode
from qsig.util.time import date_range
import qsig


def main():
    qsig.init()

    # ----------------------------------------------------------------------
    # Download raw trades from Tardis
    # ----------------------------------------------------------------------

    # Put your Tardis API key here.  If you don't have a Tardis subscription,
    # you can leave this empty, and download free data - Tardis make freely
    # available data for the first day of each month.
    api_key = ""

    # Data date range - for this example, we just stick to a single day, because
    # we don't have a Tardis key.
    date_from = dt.date(2025, 1, 1)
    date_upto = date_from + dt.timedelta(days=1)

    # universe of interest
    universe = [
        Instrument("BTC", "USDT", ExchCode.BINANCE_FUTURES),
        Instrument("XRP", "USDT", ExchCode.BINANCE_FUTURES),
        Instrument("ETH", "USDT", ExchCode.BINANCE_FUTURES),
        Instrument("BNB", "USDT", ExchCode.BINANCE_FUTURES),
        Instrument("SOL", "USDT", ExchCode.BINANCE_FUTURES),
        Instrument("DOGE", "USDT", ExchCode.BINANCE_FUTURES)]

    # describe the Tardis data files to download
    dataset = "trades"
    tardis_exchange = "binance-futures"

    # build the Tardis symbology
    tardis_symbols = [f"{x.base}{x.quote}" for x in universe]

    # create the Tardis downloader
    tardis = TardisDownloader(api_key=api_key)

    # Download Tardis data files for each date - files are only downloaded if
    # they don't already exist locally.
    logging.info("fetching Tardis CSV files ...")
    for tardis_symbol in tardis_symbols:
        for date in date_range(date_from, date_upto):
            location = tardis.fetch_csv_file(year=date.year,
                                             month=date.month,
                                             day=date.day,
                                             dataset=dataset,
                                             exchange=tardis_exchange,
                                             symbol=tardis_symbol)
            logging.info(f"csv location: {location}")

    # ----------------------------------------------------------------------
    # Create trade bins from Tardis trade files
    # ----------------------------------------------------------------------

    # Using the raw Tardis trade files, next create OHLC type trade bins.  For
    # research, it's easier to work with binned data rather than the raw trades.
    # To create the bins we need to provide the bin interval.  The files are
    # written directly to disk, as parquet files, under the tickdata folder.

    logging.info("creating trade bins ...")
    bin_rule = BarInterval("30s")  # bin trades into 30 second periods

    create_trade_bins(universe, date_from, date_upto, bin_rule)

    # ----------------------------------------------------------------------
    # Define research data to a DataRepo
    # ----------------------------------------------------------------------

    # We will use a local data repository for storing the final collection of
    # market data dataframes.

    repo = qsig.DataRepo(f"/home/{getpass.getuser()}/DATAREPO")

    # Define a library specific to our trade bin interval - this allows us to
    # create separate libraries for different intervals.
    lib = repo.get_library(f"tardis@{bin_rule}")

    # ----------------------------------------------------------------------
    # Create feature dataframes
    # ----------------------------------------------------------------------

    # Aggregate our trade bins into per-feature dataframes.  This will give
    # dataframes labelled like "open", "close" etc., where the columns are the
    # instrument symbols and the rows iterate of dates.  These per-feature
    # dataframes are often most suitable to perform quant research on multiple
    # assets simultaneously.

    features = ["open", "high", "low", "close", "buy_volume", "sell_volume",
                "volume", "count", "vwap"]

    for feature in features:
        logging.info(f"building feature '{feature}' in library '{lib.name}'")
        per_feature_dataframes = []
        for inst in universe:
            per_inst_dataframes = []
            for date in date_range(date_from, date_upto):
                # locate the previously generated trade bin file
                uri = build_trade_bin_uri(inst, date, bin_rule)
                trade_bins = pd.read_parquet(uri.path)
                per_inst_dataframes.append(trade_bins[feature])

            df = pd.concat(per_inst_dataframes)
            df.name = inst.ticker()
            per_feature_dataframes.append(df)
            del df, per_inst_dataframes

        df = pd.concat(per_feature_dataframes, axis=1)
        df.name = feature
        lib.write(feature, df)
        del df, per_feature_dataframes

    logging.info("items in repo: {}".format(", ".join(lib.list_keys())))


if __name__ == "__main__":
    main()
