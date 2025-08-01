import pandas as pd
import numpy as np
import math

# Return the total seconds for a datafame
def data_interval(data):
    idx = data.index
    return (idx[1] - idx[0]).total_seconds()

# Calculate the return from start to end, returning the value in bips
def return_bps(start: float, end: float):
    return (end-start)*1e4/start


# Given a time-series representing a signal, return a binary event version, with
# a +/- 1 for when the signal exceeds the threshold and a NaN in all other
# places
def signal_threshold(signal, threshold, fill=np.nan):
    events = pd.Series(index=signal.index, data=fill, name="events")
    events[signal > threshold] = 1
    events[signal < -threshold] = -1
    return events


def calc_fwd_returns(prices: pd.Series,
                     prices_period_sec: int,
                     study_period_sec: int,
                     as_bps=False):
    assert isinstance(prices, pd.Series)
    # number of src bins to make up the study period
    window_period = int(study_period_sec / prices_period_sec)
    assert window_period * prices_period_sec == study_period_sec
    fret = (prices.shift(-window_period) / prices) - 1.0
    return (fret * 1e4) if as_bps else fret


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


def halflife_to_span(half_life, granularity):
    """
    Convert exponential decay half-life into TA-Lib/Pandas EMA span. The
    half-life is expressed as a multiple of the granularity.  For example, if
    the half-life and the underlying data granularity are in the same units,
    granularity should be set to 1.

    Parameters:
    - half_life (float): Desired half-life of decay.
    - granularity (float): Time interval between data points.

    Returns:
    - float: Equivalent Pandas EMA span / TA-Lib timeperiod
    """
    h = half_life / granularity
    alpha = 1 - math.exp(-math.log(2) / h)
    span = (2 / alpha) - 1
    return span  # let user do the rounding


def zscore(s: pd.Series):
    z = (s - s.mean()) / s.std()
    return z
