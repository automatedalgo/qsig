import pandas as pd
import numpy as np

# Calculate the return from start to end, returning the value in bips
def return_bps(start: float, end: float):
    return (end-start)*1e4/start


# Given a time-series representing a signal, return a binary event version, with
# a 1 for when the signal exceeds the threshold and a NaN in all other places
def build_signal_events(signal, threshold, fill=np.nan):
    events = pd.Series(index=signal.index, data=fill)
    events.name = "events"
    events[signal > threshold] = 1
    return events


def calc_fwd_returns(prices: pd.Series,
                     prices_period_sec: int,
                     study_period_sec: int):
    assert isinstance(prices, pd.Series)

    # number of src bins to make up the study period
    window_period = int(study_period_sec / prices_period_sec)
    assert window_period * prices_period_sec == study_period_sec

    fret = (prices.shift(-window_period) / prices) - 1.0
    fret *= 1e4
    return fret


# Calculate the time density of a series. For example the trade volume per
# second for a specific lookback period.
def calc_density(data: pd.Series,
                 data_period_sec: int,
                 study_period_sec: int):
    assert isinstance(data, pd.Series)

    # number of src bins to make up the study period
    window_period = int(study_period_sec / data_period_sec)
    assert window_period * data_period_sec == study_period_sec

    roll_sum = data.rolling(window=window_period).sum()
    roll_den = roll_sum / study_period_sec
    return roll_den
