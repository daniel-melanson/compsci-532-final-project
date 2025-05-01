from models import (
    db,
    Stock,
    DB_FILE_PATH,
    StockDailySummary,
)
from pyspark.sql import SparkSession
import os
from analytics import init_stocks, read_data, calculate_daily_summary


def main():
    with db:
        db.create_tables(
            [Stock, StockDailySummary], safe=True
        )

    init_stocks()

    spark = SparkSession.builder.appName("AnalyticsEngine").getOrCreate()
    dataframes = read_data(spark)

    calculate_daily_summary(dataframes)
    spark.stop()


if __name__ == "__main__":
    main()
