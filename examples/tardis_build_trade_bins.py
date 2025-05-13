import datetime as dt
import pandas as pd
import getpass

from qsig.data.tardis.tardis_downloader import TardisDownloader
from qsig.data.tardis.tardis_binner import create_trade_bins, build_trade_bin_uri
from qsig.model.instrument import Instrument, ExchCode
from qsig.util.time import date_range
import qsig

def main():
    qsig.init()

    # This examples covers three aspects of working with Tardis data to do the
    # following:
    #
    # 1. fetching raw Tardis trade files
    #
    # 2. creating binned summaries (OHLC bars)
    #
    # 3. aggregation of full history into a dataframe, stored in a DataRepo
    #

    # ----------------------------------------------------------------------
    # Download raw trades from Tardis
    # ----------------------------------------------------------------------

    # Put your Tardis API key here.  If you don't have a Tardis subscription,
    # you can leave this empty, and download free data - Tardis make freely
    # available data for the first day of each month.
    api_key = ""

    # date range - for this example, we just stick to a single day, because we
    # don't have a Tardis key.
    dt_from = dt.date(2025, 2, 1)
    dt_upto = dt_from + dt.timedelta(days=1)

    # Describe the data file to download.  Symbology here is native to Tardis.
    dataset = "trades"
    tardis_exchange = "binance-futures"  # or: "binance"
    tardis_symbol = "BTCUSDT"

    # create the Tardis downloader
    tardis = TardisDownloader(api_key=api_key)

    # download Tardis datafile for each date - files are only downloaded if they
    # don't already exist locally
    for date in date_range(dt_from, dt_upto):
        location = tardis.fetch_csv_file(year=date.year,
                                         month=date.month,
                                         day=date.day,
                                         dataset=dataset,
                                         exchange=tardis_exchange,
                                         symbol=tardis_symbol)
        print(f"Tardis file saved to {location}")

    # ----------------------------------------------------------------------
    # Create bins - one file per day
    # ----------------------------------------------------------------------

    # Using the raw Tardis trade files, next create OHLC type bins.  For
    # research, it's easier to work with binned data rather than the raw trades.
    # To create the bins we need to provide the bin interval.  The files are
    # written directly to disk.

    bin_rule = "30s"  # 30 seconds
    instrument = Instrument("BTC", "USDT", ExchCode.BINANCE_FUTURES)

    create_trade_bins([instrument], dt_from, dt_upto, bin_rule)

    # ----------------------------------------------------------------------
    # Create aggregated bins
    # ----------------------------------------------------------------------

    # For research we prefer to load a single dataframe that aggregates the bins
    # across whole time period of interest.  We create that here, by loading
    # each dataframe and aggregating.
    dataframes = []

    for date in date_range(dt_from, dt_upto):
        uri = build_trade_bin_uri(instrument, date, bin_rule)
        dataframes.append(pd.read_parquet(uri.path))

    data = pd.concat(dataframes, axis=0)

    # ----------------------------------------------------------------------
    # Write research data to a DataRepo
    # ----------------------------------------------------------------------

    # Write the aggregated bins to our local data-repo directory.  Then we can
    # easily reload the dataset from other Python scripts.

    repo = qsig.DataRepo(f"/home/{getpass.getuser()}/DATAREPO")
    lib = repo.get_library("tardis")
    lib.write(f"trade_bins@{bin_rule}", data)


if __name__ == "__main__":
    main()
