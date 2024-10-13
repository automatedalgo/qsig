import logging
import datetime as dt
from typing import Optional
import requests
import json
import numpy as np
import pandas as pd
import os
from zoneinfo import ZoneInfo
from dataclasses import dataclass

from qsig.data.binance.binance_data import instrument_to_binance_feedcode, build_tick_file_uri
from qsig.model.instrument import Instrument
from qsig.model.marketdata import BarInterval, TimeUnit
from qsig.util.time import date_range

api = "https://api.binance.com"


def _date_to_datetime(
        d: dt.date,
        hour: Optional[int] = 0,
        minute: Optional[int] = 0,
        second: Optional[int] = 0) -> dt.datetime:
    return dt.datetime(d.year, d.month, d.day, hour, minute, second,
                       tzinfo=ZoneInfo("UTC"))


def _to_binance_timeunit(unit: TimeUnit):
    if unit == TimeUnit.SECOND:
        return "s"
    elif unit == TimeUnit.MINUTE:
        return "m"
    elif unit == TimeUnit.HOUR:
        return "h"
    else:
        raise ValueError(f"TimeUnit not supported for Binance, '{unit}'")


def _to_binance_interval(interval: BarInterval):
    unit = _to_binance_timeunit(interval.unit)
    binance_interval = f"{interval.count}{unit}"
    assert binance_interval in {"1s", "1m", "3m", "5m", "15m", "30m", "1h",
                                "2h", "4h", "6h", "8h", "12h"}
    return binance_interval


def call_http_fetch_klines(symbol, start_time: int, end_time: int, interval: BarInterval):
    binance_interval = _to_binance_interval(interval)
    path = "/api/v3/klines"
    options = {
        "symbol": symbol,
        "limit": 1000,
        "interval": binance_interval,
        "startTime": start_time,
        "endTime": end_time,
    }
    url = f"{api}{path}"
    logging.info("making URL request: {}, options: {}".format(url, options))
    reply = requests.get(url, params=options)
    if reply.status_code != 200:
        raise Exception(
            "http request failed, error-code {}, msg: {}".format(
                reply.status_code, reply.text
            )
        )
    return reply.text


BINANCE_CLOSE_TIME_COL_INDEX = 6


def _normalise_klines(df):
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore",
    ]

    if df is None or df.empty:
        return pd.DataFrame(columns=columns)

    assert df.shape[1] == len(columns)  # expected column count

    df.columns = columns
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]:
        df[col] = df[col].astype("float")

    df.drop(["ignore"], axis=1, inplace=True)
    return df


def _fetch_klines_for_date(symbol: str, bar_date: dt.date, interval: BarInterval):
    logging.info("fetching binance trade-bars for date {}".format(bar_date))

    # t0 and t1 and the start and end times of the date range in UTC
    t0 = _date_to_datetime(bar_date)
    t1 = _date_to_datetime(bar_date + dt.timedelta(days=1))

    all_dfs = []

    lower = int(t0.timestamp())*1000  # convert to milli-sec
    upper = int(t1.timestamp())*1000  # convert to milli-sec

    while lower < upper:

        # make the request
        req_lower = lower
        req_upper = upper
        raw_json = call_http_fetch_klines(symbol, req_lower, req_upper, interval)

        if raw_json == "":
            logging.warning(f"no JSON data retrieved for {symbol} @ {bar_date}")
            break

        df = pd.DataFrame(json.loads(raw_json))
        reply_row_count = df.shape[0]  # normally we have 1000 rows
        logging.debug(f"request returned {reply_row_count} rows")

        if df.empty:
            logging.warning(f"empty dataframe encountered for {symbol} @ {bar_date}")
            break

        # trim the returned dataframe to be within our request range, just in
        # case exchange has returned additional rows

        df = df.loc[(df[0] >= req_lower) & (df[0] < req_upper)]
        if df.shape[0] != reply_row_count:
            logging.debug(
                "retained {} rows of {} within actual request range".format(
                    df.shape[0], reply_row_count
                )
            )

        if df.empty:
            break

        all_dfs.append(df)

        lower = df.iloc[-1, BINANCE_CLOSE_TIME_COL_INDEX]
        del df, req_lower, req_upper, raw_json, reply_row_count
    del lower, upper


    df = pd.concat(all_dfs).reset_index(drop=True)
    del all_dfs
    df = _normalise_klines(df)
    if df.empty:
        logging.warning(f"no data retrieved for {symbol} @ {bar_date}")
        return df

    df = df.sort_values(by="open_time")

    # retain only rows within user requested period
    sel = (df["open_time"] >= np.datetime64(int(t0.timestamp()), 's')) & \
        (df["close_time"] < np.datetime64(int(t1.timestamp()), 's'))
    df = df[sel]

    return df


def fetch_bars_for_date(symbol: str, date: dt.date, interval: BarInterval):
    return _fetch_klines_for_date(symbol, date, interval)


@dataclass
class _BinanceBarCollectorReport:
    files_requested: int = 0
    files_already_existed: int = 0
    new_files_downloaded: int = 0


def fetch_binance_single_bar(instrument, date, interval: BarInterval, report=None):
    assert isinstance(date, dt.date)

    if isinstance(instrument, Instrument):
        feedcode = instrument_to_binance_feedcode(instrument)
    else:
        feedcode = instrument

    if report:
        report.files_requested += 1

    uri = build_tick_file_uri(instrument, date, interval)

    local_fn = uri.path
    if os.path.isfile(local_fn):
        if report:
            report.files_already_existed += 1
    else:
        os.makedirs(uri.folder, exist_ok=True)
        data = fetch_bars_for_date(feedcode, date, interval)
        logging.info(f"writing binance market-data bars to '{uri.path}'")
        data.to_parquet(uri.path)
        if report:
            report.new_files_downloaded += 1


def fetch_binance_bars(universe,
                       date_from: dt.date,
                       date_upto: dt.date,
                       bar_interval: BarInterval):
    report = _BinanceBarCollectorReport()

    for instrument in universe:
        for date in date_range(date_from, date_upto):
            fetch_binance_single_bar(instrument, date, bar_interval, report)

    logging.info(f"bar interval: {bar_interval}")
    logging.info(f"names requested: {len(universe)}")
    logging.info(f"dates requested: {(date_upto - date_from).days}")

    logging.info(f"files requested: {report.files_requested}")
    logging.info(f"files already existed: {report.files_already_existed}")
    logging.info(f"files newly downloaded: {report.new_files_downloaded}")
