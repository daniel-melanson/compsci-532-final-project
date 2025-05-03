from models import init_db
from pyspark.sql import SparkSession
from analytics import (
    init_stocks,
    read_data,
    calculate_daily_summary,
    calculate_moving_averages,
    calculate_rsi,
    calculate_bollinger_bands,
    calculate_atr,
    calculate_obv,
)


def main():
    init_db()
    init_stocks()

    spark = (
        SparkSession.builder.appName("AnalyticsEngine")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.5.jar")
        .getOrCreate()
    )
    dataframes = read_data(spark)

    # Calculate daily summaries for each stock
    summaries = calculate_daily_summary(dataframes)

    # Calculate moving averages for each stock
    calculate_moving_averages(summaries)

    # Calculate RSI for each stock
    calculate_rsi(summaries)

    # Calculate Bollinger Bands for each stock
    calculate_bollinger_bands(summaries)

    # Calculate ATR for each stock
    calculate_atr(summaries)

    # Calculate OBV for each stock
    calculate_obv(summaries)

    spark.stop()


if __name__ == "__main__":
    main()
