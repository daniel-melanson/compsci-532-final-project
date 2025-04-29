import kagglehub
import pandas as pd
import requests
import os
import tempfile
import sqlite3
from contextlib import contextmanager

DB_FILE_PATH = "nifty_500_analytics.db"
NIFTY_500_CSV = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
NIFTY_TRADING_DATA_URL = (
    "debashis74017/algo-trading-data-nifty-100-data-with-indicators"
)


@contextmanager
def db_connection(db_path=DB_FILE_PATH):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def db_cursor(db_path=DB_FILE_PATH):
    with db_connection(db_path) as conn:
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        finally:
            cursor.close()


def init():
    with db_cursor() as c:
        c.execute(
            """
        CREATE TABLE stocks (
            id INTEGER PRIMARY KEY,
            company_name TEXT NOT NULL,
            industry TEXT NOT NULL,
            symbol TEXT NOT NULL UNIQUE,
            series TEXT NOT NULL,
            isin_code TEXT NOT NULL UNIQUE
        );
        """
        )

        c.execute("CREATE INDEX idx_symbol ON stocks (symbol);")
        c.execute("CREATE INDEX idx_industry ON stocks (industry);")
        c.execute("CREATE INDEX idx_company_name ON stocks (company_name);")

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
    with db_cursor() as c:
        c.executemany(
            "INSERT INTO stocks (company_name, industry, symbol, series, isin_code) VALUES (?, ?, ?, ?, ?)",
            df.itertuples(index=False),
        )

    # data_path = kagglehub.dataset_download(NIFTY_TRADING_DATA_URL)
