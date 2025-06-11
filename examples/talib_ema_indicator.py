import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from qsig.indicators import IndicatorCache, IndicatorFactory

# ------------------------------------------------------------------------------
# Standalone demonstration of using TA-Lib EMA indicator decay, for a couple of
# decay rates.  We can use the graph to confirm the decay behaves as expected.
# ------------------------------------------------------------------------------

# check TA-Lib is available
assert IndicatorFactory.has_talib(), "TA-Lib not installed"

# Create a time index (1-minute intervals)
index = pd.date_range(start="2024-01-01 06:00:00", periods=4*60, freq='min')

# Create time series: starts at 100, then drops to 0
data = pd.DataFrame(index=index)
data["close"] = 0.0
data[0:3*60 + 1] = 100.0

# Create indicator infrastructure
factory = IndicatorFactory()
cache = IndicatorCache(instrument=None)
cache.add_data(data.ffill())

# Create indicators
cache.add_indicator(f"SMA_5=SMA(5m)", container=cache)
cache.add_indicator(f"EMA_5=EMA(5m)", container=cache)
cache.add_indicator(f"EMA_10=EMA(10m)", container=cache)

# Compute the indicator
cache.compute()

# Collect results & plotting
df = cache.to_frame()
df = df[2*60+45:int(3.5*60)]

plt.figure(figsize=(10, 5))
plt.plot(df.index, df['SMA_5'], label='SMA_5', linestyle='--')
plt.plot(df.index, df['EMA_5'], label='EMA_5', linestyle='-')
plt.plot(df.index, df['EMA_10'], label='EMA_10', linestyle='-')
plt.axhline(y=50, color='red', linestyle='--', linewidth=1)
plt.title('Indicator Response of SMA and TA-Lib EMA to a Step Change from 100 to 0')
plt.xlabel('Time')
plt.ylabel('Value')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
