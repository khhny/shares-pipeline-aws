import boto3
import json
import time
import yfinance as yf
import pandas as pd
from datetime import datetime, UTC

STREAM_NAME = "stock-price-stream"
TICKERS = ["NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "AVGO", "GOOG", "META", "TSLA", "MU"]
REGION = "us-east-1"

kinesis = boto3.client("kinesis", region_name = "us-east-1")

def get_stock_data():
    #Download recent stock data to connect ot stream

    df = yf.download(
        tickers=TICKERS,
        period="60d",
        interval="1d",
        auto_adjust=True,
        progress=False
    )

    df = df.stack(level=1).reset_index()

    df = df.dropna()
    df["Date"] = df["Date"].astype(str)

    return df.sort_values("Date")


def compute_features(row, history_df):
    #Compute features for each incoming tick

    ticker_hist = history_df[
        history_df["Ticker"] == row["Ticker"] &
        history_df["Date"] <= row["Date"]
    ].tail(20)

    sma_20 = ticker_hist["close"].mean() if len(ticker_hist) >= 5 else None
    price_range = row["high"] - row["low"]
    daily_return = (row["close"] - row["open"]) / row["open"] * 100

    #Momentum = close vs 5day average
    sma_5 = ticker_hist["Close"].mean() if len(ticker_hist) >= 5 else None
    momentum = row["Close"] - sma_5 if sma_5 else None

    return {
        "event_time": datetime.now(UTC).isoformat(),
        "Date": row["Date"],
        "Ticker": row["Ticker"],
        "Open": round(float(row["Open"]), 4),
        "High": round(float(row["High"]), 4),
        "Low": round(float(row["Low"]), 4),
        "Close": round(float(row["Close"]), 4),
        "Volume": int(row["Volume"]),
        "daily_return_pct": round(daily_return, 4),
        "price_range": round(price_range, 4),
        "sma_20": round(sma_20, 4) if sma_20 else None,
        "sma_5": round(float(sma_5), 4) if sma_5 else None,
        "momentum_5d": round(float(momentum), 4) if momentum else None,
        # Label for ML training (will close be higher tomorrow?)
        # Note: in real-time you won't know this — added here for training data
        "above_sma20": 1 if (sma_20 and row["Close"] > sma_20) else 0
    }




def send_to_kinesis(record):
    #Send 1 record to KDS

    response = kinesis.put_record(
        StreamName = STREAM_NAME,
        Data = json.dumps(record),
        PartitionKey = record["Ticker"]
    )

    return response["ShardId"]

if __name__ == "__main__":
    print("Downloading data")
    df = get_stock_data()

    print("Streaming")

    for c, row in df.iterrows():
        record = compute_features(row, df)
        shard = send_to_kinesis(record)

        print(f"Sent {record['ticker']} {record['date']}"
              f"close={record['close']}"
              f"return={record['daily_return_pct']}%: {shard}")
        time.sleep(0.5)

    print("Stream complete")
