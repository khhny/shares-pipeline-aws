import pandas as pd
import boto3
import io
from datetime import datetime
import yfinance as yf

#Listing stocks to keep track of
#vanguard S&P ETF top 10
TICKER = ["NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "AVGO", "GOOG", "META", "TSLA", "MU"]
S3_BUCKET = "henny-stock-raw"

def main():
    s3 = boto3.client("s3")
    today = datetime.today().strftime("%Y-%m-%d")

    print("Downloading yf data")
    df = yf.download(
        tickers=TICKER,
        start="2022-01-01",
        end=today,
        interval="1d",
        auto_adjust=True,
        progress=False
    )

    #Transform dataframe
    df = df.stack(level=1).reset_index()

    df.columns = ["Date", "Ticker", "Close", "High", "Low", "Open", "Volume"]
    df = df.dropna()
    df["Date"] = df["Date"].astype(str)

    #Upload 2 S3
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)

    S3_KEY = f"daily_prices/run_date={today}/stocks.csv"
    s3.put_object(
        Bucket = S3_BUCKET,
        Key = S3_KEY,
        Body = buffer.getvalue()
    )

    print(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")

    print(df.head())

if __name__ == "__main__":
    main()
    pass



