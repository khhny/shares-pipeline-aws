import sys
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime

BUCKET_RAW = "henny-stock-raw"
BUCKET_PROCESSED = "henny-stock-processed"

RAW_PREFIX = "daily_prices/"
OUTPUT_PREFIX = "transformed/"

s3 = boto3.client("s3")

def readcsvs():
    paginator = s3.get_paginator("list_objects_v2")
    dfs = []

    for page in paginator.paginate(Bucket = BUCKET_RAW, Prefix = RAW_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                response = s3.get_object(Bucket = BUCKET_RAW, Key = obj["Key"])
                df = pd.read_csv(response["Body"])
                dfs.append(df)
            
    return pd.concat(dfs, ignore_index = True)

def transform(df):
    #ETL Logic
    df["Date"] = pd.to_datetime(df["Date"])

    #Add daily return percentage
    df = df.dropna(subset=["Open", "Close", "High", "Low", "Volume"])
    df["daily_return_percentage"] = ((df["Close"] - df["Open"]) / df["Open"] * 100).round(4)

    #Add price range
    df["price_range"] = (df["High"] - df["Low"]).round(4)

    #Add flag for high-volume dates
    avg_vol = df.groupby("Ticker")["Volume"].transform("mean")
    df["high_volume_day"] = (df["Volume"] > avg_vol * 2).astype(int)

    #Add 20-day rolling average close price per ticker
    df = df.sort_values(["Ticker", "Date"])
    df["sma_20"] = df.groupby("Ticker")["Close"].transform(
        lambda x: x.rolling(window=20, min_periods=1).mean()
    ).round(4)

    #Add year and month columns for partitions
    df["year"] = df["Date"].dt.year
    df["month"] = df["Date"].dt.month

    return df


def write_parquet(df):
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)
    buffer.seek(0)

    today = datetime.today().strftime("%Y-%m-%d")

    key = f"{OUTPUT_PREFIX}run_date={today}/stocks_transformed.parquet"
    s3.put_object(Bucket=BUCKET_PROCESSED, Key=key, Body=buffer.getvalue())
    print(f"Wrote to s3://{BUCKET_PROCESSED}/{key}")


df = readcsvs()
print("Loading rows")

df = transform(df)
print(f"{len(df)} rows \n Columns: {list(df.columns)}")
write_parquet(df)
print("done")