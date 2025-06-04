import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Standalone demonstration of pure ewma decay, to compare to indicator
# implementations

# Create a time index (1-minute intervals)
index = pd.date_range(start="2024-01-01 09:00:00", periods=30, freq='min')

# Create time series: starts at 100, then drops to 0
df = pd.DataFrame(index=index)
df["close"] = 0.0
df[0:11] = 100.0

# Define smoothing window
window = 5  # 5-minute equivalent window

# Compute SMA and EWMA
df['SMA'] = df['close'].rolling(window=window).mean()
df['EWMA'] = df['close'].ewm(halflife=window, adjust=False).mean()

# Plotting
plt.figure(figsize=(10, 5))
plt.plot(df.index, df['close'], label='Original', marker='o')
plt.plot(df.index, df['SMA'], label='5-min SMA', linestyle='--')
plt.plot(df.index, df['EWMA'], label='5-min EWMA', linestyle='-')
plt.axhline(y=50, color='red', linestyle='--', linewidth=1)
plt.title('Response of SMA and EWMA to a Step Change from 100 to 0')
plt.xlabel('Time')
plt.ylabel('Value')
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
