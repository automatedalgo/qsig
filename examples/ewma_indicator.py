import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from qsig.indicators import IndicatorCache, IndicatorFactory

# ------------------------------------------------------------------------------
# Standalone demonstration of EWMA indicator decay, for a couple of decay rates.
# We can use the graph to confirm the decay behaves as expected.
# ------------------------------------------------------------------------------

# Create a time index (1-minute intervals)
index = pd.date_range(start="2024-01-01 09:00:00", periods=30, freq='min')

# Create time series: starts at 100, then drops to 0
data = pd.DataFrame(index=index)
data["close"] = 0.0
data[0:11] = 100.0

# Create indicator infrastructure
factory = IndicatorFactory()
cache = IndicatorCache(instrument=None)
cache.add_data(data)

# Create indicators
cache.add_indicator(f"SMA_5=SMA(5m)[close]", container=cache)
cache.add_indicator(f"EWMA_5=EWMA(5m)[close]", container=cache)
cache.add_indicator(f"EWMA_10=EWMA(10m)[close]", container=cache)

# Compute the indicator
cache.compute()

# Collect results
df = cache.to_frame()

# Plotting
plt.figure(figsize=(10, 5))
plt.plot(df.index, df['SMA_5'], label='SMA_5', linestyle='--')
plt.plot(df.index, df['EWMA_5'], label='EWMA_5', linestyle='-')
plt.plot(df.index, df['EWMA_10'], label='EWMA_10', linestyle='-')
plt.axhline(y=50, color='red', linestyle='--', linewidth=1)
plt.title('Indicator Response of SMA and EWMA to a Step Change from 100 to 0')
plt.xlabel('Time')
plt.ylabel('Value')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
