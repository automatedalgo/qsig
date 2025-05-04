import datetime as dt
import pandas as pd
import logging
from typing import List
import os

from qsig.model.instrument import Instrument, Exchange_Map
from qsig.util.time import date_range
from qsig.data.tickfiles import TickFileURI

# Read a raw tardis CSV file, as its looks after downloading from Tardis.
def read_raw_csv_tick_data(filename: str,
                           to_datetime=True,
                           set_index: str = "local_timestamp"):
    logging.info(f"reading tardis tick-data file '{filename}'")

    if set_index is not None:
        assert set_index in ["timestamp", "local_timestamp"]

    data = pd.read_csv(filename, compression='gzip')
    if to_datetime:
        for col in ["timestamp", "local_timestamp"]:
            data[col] = pd.to_datetime(data[col], unit="us")
    if set_index is not None:
        data = data.set_index(set_index).sort_index()
    return data


# Create a DataFrame of trade bins from a DataFrame of raw trades.
def calc_trade_bins(date: dt.date, trades: pd.DataFrame, rule: str) -> pd.DataFrame:
    # value values for `rule` are <N>s or N<min> or N<h>
    assert rule.endswith("min") or rule.endswith("s") or rule.endswith("h")

    # calc simple derived values
    trades["value"] = trades["price"] * trades["amount"]
    trades["is_buy"] = trades["side"].apply(lambda x: 1 if x == "buy" else 0)
    trades["is_sell"] = trades["side"].apply(lambda x: 1 if x == "sell" else 0)
    trades["buy_amount"] = trades["is_buy"] * trades["amount"]
    trades["sell_amount"] = trades["is_sell"] * trades["amount"]

    # Create the resampler on specified time-period rule.  The label is set to
    # 'right', to reduce accidental lookahead.
    resampler = trades.resample(rule=rule, closed="left", label="right")

    # calc resampled values
    ohlc = resampler["price"].ohlc()
    count = resampler["price"].count()
    count.name = "count"
    volume = resampler["amount"].sum()  # interval volume
    volume.name = "volume"
    buy_volume = resampler["buy_amount"].sum()
    buy_volume.name = "buy_volume"
    sell_volume = resampler["sell_amount"].sum()
    sell_volume.name = "sell_volume"
    value = resampler["value"].sum()  # interval value
    mean = resampler["price"].mean()
    mean.name = "mean"
    std = resampler["price"].std()
    std.name = "std"

    # construct the final data item
    final = pd.concat([ohlc, mean, std, count, volume, value, buy_volume, sell_volume], axis=1)
    final["vwap"] = final["value"] / final[volume.name]
    final = final.drop(["value"], axis=1)

    # apply a normalised datetime index
    from_ts = dt.datetime(year=date.year, month=date.month, day=date.day)
    upto_ts = from_ts + dt.timedelta(days=1)
    idx = pd.date_range(from_ts, upto_ts, freq=rule, inclusive="right")
    final = final.reindex(idx)

    # for volume/count related columns, fill(0) and cast to int where
    # appropriate
    for col in ["volume", "buy_volume", "sell_volume", "count"]:
        final[col] = final[col].fillna(0.0)

    final["count"] = final["count"].astype(int)
    return final


def _create_trade_bins_file(trades_uri: TickFileURI,
                            bins_uri: TickFileURI,
                            bin_rule: str):
    if os.path.isfile(bins_uri.path):
        logging.info(f"trades bin already exists, {bins_uri.path}")
        return

    # read the raw ticks
    trades = read_raw_csv_tick_data(trades_uri.path.as_posix(), to_datetime=True)

    # create the dataframe of trade bins
    bins = calc_trade_bins(date=bins_uri.date, trades=trades, rule=bin_rule)

    # write the data
    os.makedirs(bins_uri.folder, exist_ok=True)
    logging.info(f"writing trade bins file '{bins_uri.path}'")
    bins.to_parquet(bins_uri.path)


def create_trade_bins(instruments: List[Instrument],
                      date_from: dt.date,
                      date_upto: dt.date,
                      bin_rule: str):
    for date in date_range(date_from, date_upto):
        for inst in instruments:
            symbol = f"{inst.base}{inst.quote}"
            logging.info(f"generating bins for {inst} on {date}")

            # location of the input trades file
            input_uri = TickFileURI(
                filename=f"{symbol}.csv.gz",
                collection="tardis",
                venue=Exchange_Map[inst.exch].slug,
                dataset="trades",
                date=date,
                symbol=symbol)

            # location of the output bin file
            output_uri = input_uri.replace(
                collection="qsig",
                dataset=f"{input_uri.dataset}@{bin_rule}",
                filename=f"{input_uri.symbol}.parquet"
            )

            _create_trade_bins_file(input_uri, output_uri, bin_rule)
