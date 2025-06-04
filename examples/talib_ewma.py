import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import talib

from qsig.util.signal import halflife_to_span

# ------------------------------------------------------------------------------
# Standalone demonstration of pure ewma decay, using both ta-TA-Lib and Pandas.
# ------------------------------------------------------------------------------

# Create a time index (1-minute intervals)
index = pd.date_range(start="2024-01-01 08:30:00", periods=60, freq='min')

# Create time series: starts at 100, then drops to 0
data = pd.DataFrame(index=index)
data["close"] = 0.0
data[0:31] = 100.0

# Generate the ema decays - note that talib.EMA takes an integer, so we round.
data["EMA_5"] = talib.EMA(data["close"],  round(halflife_to_span(5, 1)))
data["EMA_10"] = talib.EMA(data["close"], round(halflife_to_span(10, 1)))
data['pd.ema_5'] = data['close'].ewm(span=halflife_to_span(5, 1), adjust=False).mean()
data['pd.ema_10'] = data['close'].ewm(span=halflife_to_span(10, 1), adjust=False).mean()

# Plotting
df = data[20:]
plt.figure(figsize=(10, 5))
plt.plot(df.index, df['close'], label='close', linestyle='-')
plt.plot(df.index, df['EMA_5'], label='talib.EMA_5', linestyle='-')
plt.plot(df.index, df['EMA_10'], label='talib.EMA_10', linestyle='-')
plt.plot(df.index, df['pd.ema_5'], label='pd.ema_5', linestyle='-.')
plt.plot(df.index, df['pd.ema_10'], label='pd.ema_10', linestyle='-.')
plt.axhline(y=50, color='red', linestyle='--', linewidth=1)
plt.title('EWMA comparison for TA-Lib and Pandas to a Step Change from 100 to 0')
plt.xlabel('Time')
plt.ylabel('Value')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
