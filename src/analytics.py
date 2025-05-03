from pyspark.sql.functions import col, first, max, min, last, sum, lit, when, avg
from pyspark.sql.window import Window
from models import Stock, StockDailySummary, StockMovingAverage, DB_NAME
import os
import requests
import pandas as pd
import re
import kagglehub

DEBUG = os.getenv("DEBUG", False)
NIFTY_500_CSV = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
NIFTY_500_CSV_PATH = "data/nifty_500.csv"
NIFTY_TRADING_DATA_URL = (
    "debashis74017/algo-trading-data-nifty-100-data-with-indicators"
)

""""
Stocks: Daily percent change, daily absolute change, rolling 50-day average, sector weight 
Sectors: Daily percent change, daily absolute change, rolling 50-day average, market capitalization
"""

def init_stocks():
    if not os.path.exists(NIFTY_500_CSV_PATH):
        response = requests.get(
            NIFTY_500_CSV,
            headers={
                "User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.10 Safari/605.1.1"
            },
        )
        response.raise_for_status()

        with open(NIFTY_500_CSV_PATH, "wb") as f:
            f.write(response.content)

    df = pd.read_csv(NIFTY_500_CSV_PATH, header=0)
    df = df.rename(
        columns={
            "Company Name": "company_name",
            "Industry": "industry",
            "Symbol": "symbol",
            "Series": "series",
            "ISIN Code": "isin_code",
        }
    )

    Stock.insert_many(df.to_dict(orient="records")).on_conflict_ignore().execute()


def read_data(spark):
    data_path = kagglehub.dataset_download(NIFTY_TRADING_DATA_URL)

    csv_files = [f for f in os.listdir(data_path) if f.endswith(".csv")]

    dataframes = {}
    for file in csv_files:
        file_path = os.path.join(data_path, file)
        symbol = re.search(r"(\w+)_minute\.csv", file).group(1)
        if not symbol or not Stock.select().where(Stock.symbol == symbol).exists():
            continue

        df = (
            spark.read.option("header", "true")
            .csv(file_path)
            .withColumn("symbol", lit(symbol))
            .withColumn("open", col("open").cast("float"))
            .withColumn("high", col("high").cast("float"))
            .withColumn("low", col("low").cast("float"))
            .withColumn("close", col("close").cast("float"))
            .withColumn("volume", col("volume").cast("float"))
            .withColumn("datetime", col("date").cast("timestamp"))
            .withColumn("date", col("datetime").cast("date"))
        )
        dataframes[symbol] = df
        if DEBUG:
            break

    return dataframes


def iterate_dataframes(dataframes):
    for symbol, df in dataframes.items():
        yield symbol, df
        if DEBUG:
            break


def save_to_db(df, model):
    df.write.format("jdbc").option("url", f"jdbc:postgresql://db/{DB_NAME}").option(
        "dbtable", f"public.{model.__name__}"
    ).option("user", "postgres").mode("overwrite").option("password", "postgres").save()

    # batch_size = 1000
    # for i in range(0, df.count(), batch_size):
    #     batch_df = df.limit(batch_size).offset(i).toPandas()
    #     model.insert_many(batch_df.to_dict(orient="records")).on_conflict_ignore().execute()


def calculate_daily_summary(dataframes):
    StockDailySummary.delete().execute()

    summary_dfs = {}

    for symbol, df in iterate_dataframes(dataframes):
        df = (
            df.orderBy("datetime")
            .groupBy("date")
            .agg(
                first(col("open")).alias("open"),
                max(col("high")).alias("high"),
                min(col("low")).alias("low"),
                last(col("close")).alias("close"),
                sum(col("volume")).alias("volume"),
            )
            .withColumn("symbol", lit(symbol))
            .withColumn("absolute_change", col("close") - col("open"))
            .withColumn(
                "percentage_change",
                when(
                    col("open") != 0, (col("close") - col("open")) / col("open")
                ).otherwise(0.0),
            )
        )

        summary_dfs[symbol] = df
        save_to_db(df, StockDailySummary)

    return summary_dfs


def calculate_moving_averages(summaries):
    StockMovingAverage.delete().execute()

    for symbol, df in iterate_dataframes(summaries):
        df = (
            df.withColumn(
                "moving_average",
                avg(col("close")).over(
                    Window.partitionBy("symbol").orderBy("date").rowsBetween(-50, 0)
                ),
            )
            .withColumn("period", lit(50))
            .drop(
                "open",
                "high",
                "low",
                "close",
                "volume",
                "absolute_change",
                "percentage_change",
                "datetime",
            )
        )

        save_to_db(df, StockMovingAverage)
