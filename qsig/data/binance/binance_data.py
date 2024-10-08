import datetime
import logging
import pandas as pd
from typing import Union, List

from qsig.data.tickfiles import TickFileURI
from qsig.model.instrument import Instrument
from qsig.model.marketdata import BarInterval
from qsig.util.time import date_range


# This file stores common data processing functions and classes

def instrument_to_binance_feedcode(inst: Instrument):
    assert isinstance(inst, Instrument)
    return f"{inst.base}{inst.quote}"


def build_tick_file_uri(instrument: Union[Instrument, str],
                        date: datetime.date,
                        interval: BarInterval):
    assert isinstance(date, datetime.date)

    if isinstance(instrument, Instrument):
        feedcode = instrument_to_binance_feedcode(instrument)
    else:
        feedcode = instrument

    dataset = f"trades@{interval}"

    uri = TickFileURI(filename=f"{feedcode}.parquet",
                      collection="binance",
                      venue="binance",
                      dataset=dataset,
                      date=date,
                      symbol=feedcode)
    return uri


def build_binance_trade_features_dataset(universe: List[Instrument],
                                         date_from,
                                         date_upto,
                                         bar_interval,
                                         lib,
                                         skip_missing_files=False):
    features = ['open', 'high', 'low', 'close', 'return']

    # To build the aggregated features dataframe, we initially build each
    # feature for a single name, over its full history.  So below the outer loop
    # is the instrument list, and the inner loop is the date list.
    for inst in universe:
        feature_map = {x: [] for x in features}
        logging.info(f"building features for {inst}")
        for date in date_range(date_from, date_upto):
            uri = build_tick_file_uri(inst, date, bar_interval)

            # check for presence of raw market data
            try:
                logging.info(f"reading binance market-data bars '{uri.path}'")
                df = pd.read_parquet(uri.path)
            except FileNotFoundError as e:
                if skip_missing_files:
                    logging.warning(f"missing binance file '{uri.path}'")
                    continue
                else:
                    raise e

            # convert the binance bar to  qsig bar on-the-fly
            # qsig bar convention is to label bins with the bar close time

            df["time"] = df["open_time"] + bar_interval.to_pandas_timedelta()
            df = df.set_index("time", verify_integrity=True)

            for feature_name in features:
                if feature_name != "return":
                    feature_map[feature_name].append(df[feature_name])
            del feature_name, df, uri

        # now that we have features for each date in the date-range, we combine
        # the to get the full history
        for feature_name in features:
            full_hist = pd.Series()
            if feature_name == "return":
                if len(feature_map["close"]) > 0:
                    full_hist = pd.concat(feature_map["close"]).pct_change(periods=1, fill_method=None)
            else:
                if len(feature_map[feature_name]) > 0:
                    full_hist = pd.concat(feature_map[feature_name])
            full_hist.name = inst.ticker()
            part_name = f"_part.{feature_name}.{inst.ticker()}"
            logging.info(f"writing item part: {part_name}")
            lib.write(part_name, full_hist.to_frame())
            del full_hist
        del feature_map

    # Build the final features dataframes - that is, have a dataframe for
    # "close", "open" etc.  This is done by concatenating the full history of
    # each feature for all names into a single per-feature dataframe.
    for feature_name in features:
        dfs = []
        for inst in universe:
            part_name = f"_part.{feature_name}.{inst.ticker()}"
            df = lib.read(part_name)
            if not df.empty:
                dfs.append(df)
            lib.delete(part_name)

        final = pd.concat(dfs, axis=1)

        # in case some symbols are entirely missing, put them back in with NaN
        # data
        expected_cols = [inst.ticker() for inst in universe]
        final = final.reindex(columns=expected_cols)

        logging.info(f"writing final item '{feature_name}'")
        lib.write(feature_name, final)
