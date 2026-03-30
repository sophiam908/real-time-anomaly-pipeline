import yfinance as yf
import pandas as pd

# download stock data (Apple)
df = yf.download("AAPL", start="2022-01-01", end="2024-01-01")

# save to CSV
df.to_csv("data/stock_data.csv")

print("Saved stock_data.csv")