from models import db, Stock, DB_FILE_PATH, StockDailyChange
from pyspark.sql import SparkSession
import requests
import tempfile
import pandas as pd
import os
import kagglehub


NIFTY_500_CSV = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
NIFTY_TRADING_DATA_URL = (
    "debashis74017/algo-trading-data-nifty-100-data-with-indicators"
)


def init_database():
    db.connect()
    db.create_tables([Stock, StockDailyChange])

    response = requests.get(
        NIFTY_500_CSV,
        headers={
            "User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.10 Safari/605.1.1"
        },
    )
    response.raise_for_status()

    fd, path = tempfile.mkstemp()
    os.write(fd, response.content)

    df = pd.read_csv(path, header=0)
    df = df.rename(
        columns={
            "Company Name": "company_name",
            "Industry": "industry",
            "Symbol": "symbol",
            "Series": "series",
            "ISIN Code": "isin_code",
        }
    )
    Stock.insert_many(df.to_dict(orient="records")).execute()


def main():
    if not os.path.exists(DB_FILE_PATH):
        init_database()

    data_path = kagglehub.dataset_download(NIFTY_TRADING_DATA_URL)

    spark = SparkSession.builder.appName("AnalyticsEngine").getOrCreate()

    csv_files = [f for f in os.listdir(data_path) if f.endswith(".csv")]

    dataframes = {}
    for file in csv_files:
        file_path = os.path.join(data_path, file)
        df = spark.read.option("header", "true").csv(file_path)
        dataframes[file] = df

    print(dataframes)
    spark.stop()


if __name__ == "__main__":
    main()
