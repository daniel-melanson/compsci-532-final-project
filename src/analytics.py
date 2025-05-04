from pyspark.sql.functions import (
    col,
    first,
    greatest,
    max,
    min,
    last,
    sum,
    lit,
    when,
    avg,
    lag,
    abs,
    stddev,
)
from pyspark.sql.window import Window
from prometheus_client import Summary
from models import (
    Stock,
    StockDailySummary,
    StockMovingAverage,
    DB_NAME,
    StockRSI,
    StockBollingerBand,
    StockATR,
    StockOBV,
)
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

init_stocks_summary = Summary("init_stocks", "Time taken to initialize stocks")


@init_stocks_summary.time()
def init_stocks():
    """
    Initialize the stocks in the database with sectors.
    """
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


read_data_summary = Summary("read_data", "Time taken to read data")


@read_data_summary.time()
def read_data(spark):
    """
    Read the data from the Kaggle dataset. Caches the data in the file system.

    Loads the data and returns a dictionary of DataFrames.
    """
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
            .orderBy("datetime")
        )
        dataframes[symbol] = df
        if DEBUG:
            break

    return dataframes


def iterate_dataframes(dataframes):
    """
    Iterate through the dataframes.
    """
    for symbol, df in dataframes.items():
        yield symbol, df
        if DEBUG:
            break


save_to_db_summary = Summary("save_to_db", "Time taken to save to database")


@save_to_db_summary.time()
def save_to_db(df, model):
    """
    Save a DataFrame to a PostgreSQL database table.
    """
    df.write.format("jdbc").option("url", f"jdbc:postgresql://db/{DB_NAME}").option(
        "dbtable", f"public.{model.__name__}"
    ).option("user", "postgres").mode("overwrite").option("password", "postgres").save()

    # batch_size = 1000
    # for i in range(0, df.count(), batch_size):
    #     batch_df = df.limit(batch_size).offset(i).toPandas()
    #     model.insert_many(batch_df.to_dict(orient="records")).on_conflict_ignore().execute()


calculate_daily_summary_summary = Summary(
    "calculate_daily_summary", "Time taken to calculate daily summary"
)


@calculate_daily_summary_summary.time()
def calculate_daily_summary(dataframes):
    # Daily Summary = Open, High, Low, Close, Volume, Absolute Change, Percentage Change
    # Absolute Change = Close - Open
    # Percentage Change = (Close - Open) / Open

    StockDailySummary.delete().execute()

    summary_dfs = {}

    for symbol, df in iterate_dataframes(dataframes):
        df = (
            df.groupBy("date")
            .agg(
                first(col("open")).alias("open"),
                max(col("high")).alias("high"),
                min(col("low")).alias("low"),
                last(col("close")).alias("close"),
                sum(col("volume")).alias("volume"),
            )
            .withColumn("symbol", lit(symbol))
            .withColumn(
                "prev_close",
                lag("close").over(Window.partitionBy("symbol").orderBy("date")),
            )
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


calculate_moving_averages_summary = Summary(
    "calculate_moving_averages", "Time taken to calculate moving averages"
)


@calculate_moving_averages_summary.time()
def calculate_moving_averages(summaries, period=50):
    # Moving Average = (Sum of Close Prices) / (Number of Days)
    StockMovingAverage.delete().execute()

    for symbol, df in iterate_dataframes(summaries):
        df = (
            df.withColumn(
                "moving_average",
                avg(col("close")).over(
                    Window.partitionBy("symbol").orderBy("date").rowsBetween(-period, 0)
                ),
            )
            .withColumn("period", lit(period))
            .drop(
                "open",
                "high",
                "low",
                "close",
                "volume",
                "absolute_change",
                "percentage_change",
                "datetime",
                "prev_close",
            )
        )

        save_to_db(df, StockMovingAverage)


calculate_rsi_summary = Summary("calculate_rsi", "Time taken to calculate RSI")


@calculate_rsi_summary.time()
def calculate_rsi(summaries):
    # RSI = 100 - (100 / (1 + RS))
    # Where RS = Average Gain / Average Loss over N periods (N=14)
    StockRSI.delete().execute()

    for symbol, df in iterate_dataframes(summaries):
        window_14d = Window.partitionBy("symbol").orderBy("date").rowsBetween(-13, 0)

        df = (
            df.withColumn("price_change", col("close") - col("prev_close"))
            .withColumn(
                "gain", when(col("price_change") > 0, col("price_change")).otherwise(0)
            )
            .withColumn(
                "loss",
                when(col("price_change") < 0, abs(col("price_change"))).otherwise(0),
            )
            .withColumn("avg_gain_14", avg("gain").over(window_14d))
            .withColumn("avg_loss_14", avg("loss").over(window_14d))
            .withColumn(
                "rs",
                when(
                    col("avg_loss_14") != 0, col("avg_gain_14") / col("avg_loss_14")
                ).otherwise(lit(0)),
            )
            .withColumn(
                "rsi_14",
                when(col("avg_loss_14") != 0, 100 - (100 / (1 + col("rs")))).otherwise(
                    100
                ),
            )
            .withColumn("period", lit(14))
            .drop(
                "open",
                "high",
                "low",
                "close",
                "volume",
                "absolute_change",
                "percentage_change",
                "datetime",
                "prev_close",
                "price_change",
                "gain",
                "loss",
                "avg_gain_14",
                "avg_loss_14",
                "rs",
            )
        )

        save_to_db(df, StockRSI)


calculate_bollinger_bands_summary = Summary(
    "calculate_bollinger_bands", "Time taken to calculate Bollinger Bands"
)


@calculate_bollinger_bands_summary.time()
def calculate_bollinger_bands(summaries):
    # Middle Band = 20-day SMA
    # Upper Band = Middle Band + (20-day standard deviation × 2)
    # Lower Band = Middle Band - (20-day standard deviation × 2)
    StockBollingerBand.delete().execute()

    for symbol, df in iterate_dataframes(summaries):
        window_20d = Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0)

        df = (
            df.withColumn("sma_20", avg("close").over(window_20d))
            .withColumn("std_dev_20", stddev("close").over(window_20d))
            .withColumn("bollinger_upper", col("sma_20") + (col("std_dev_20") * 2))
            .withColumn("bollinger_lower", col("sma_20") - (col("std_dev_20") * 2))
            .drop(
                "open",
                "high",
                "low",
                "close",
                "volume",
                "absolute_change",
                "percentage_change",
                "sma_20",
                "std_dev_20",
                "datetime",
                "prev_close",
            )
        )

        save_to_db(df, StockBollingerBand)


calculate_atr_summary = Summary("calculate_atr", "Time taken to calculate ATR")


@calculate_atr_summary.time()
def calculate_atr(summaries):
    # True Range = Max(high - low, abs(high - prev_close), abs(low - prev_close))
    # ATR = Average of True Range over N periods (N=14)

    StockATR.delete().execute()

    for symbol, df in iterate_dataframes(summaries):
        window_14d = Window.partitionBy("symbol").orderBy("date").rowsBetween(-13, 0)
        df = (
            df.withColumn(
                "true_range",
                greatest(
                    col("high") - col("low"),
                    abs(col("high") - col("prev_close")),
                    abs(col("low") - col("prev_close")),
                ),
            )
            .withColumn("atr", avg("true_range").over(window_14d))
            .withColumn("period", lit(14))
            .drop(
                "open",
                "high",
                "low",
                "close",
                "volume",
                "absolute_change",
                "percentage_change",
                "prev_close",
                "true_range",
            )
        )

        save_to_db(df, StockATR)


calculate_obv_summary = Summary("calculate_obv", "Time taken to calculate OBV")


@calculate_obv_summary.time()
def calculate_obv(summaries):
    # OBV = Previous OBV +
    #       - Current Volume (if close < prev_close)
    #       - 0 (if close = prev_close)
    #       - Current Volume (if close > prev_close)
    StockOBV.delete().execute()

    for symbol, df in iterate_dataframes(summaries):
        df = (
            df.withColumn(
                "volume_direction",
                when(col("close") > col("prev_close"), col("volume"))
                .when(col("close") < col("prev_close"), -col("volume"))
                .otherwise(0),
            )
            .withColumn(
                "obv",
                sum("volume_direction").over(
                    Window.partitionBy("symbol")
                    .orderBy("date")
                    .rowsBetween(Window.unboundedPreceding, 0)
                ),
            )
            .drop(
                "open",
                "high",
                "low",
                "close",
                "volume",
                "absolute_change",
                "percentage_change",
                "prev_close",
                "volume_direction",
            )
        )

        save_to_db(df, StockOBV)
