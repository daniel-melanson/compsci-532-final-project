from models import init_db
from pyspark.sql import SparkSession
from analytics import (
    init_stocks,
    read_data,
    calculate_daily_summary,
    calculate_moving_averages,
    calculate_rsi,
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

    spark.stop()


if __name__ == "__main__":
    main()
