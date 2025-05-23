from playhouse.postgres_ext import *
import psycopg2

DB_NAME = "analytics_engine"
conn = psycopg2.connect(
    host="db", user="postgres", password="postgres", database="postgres"
)
conn.autocommit = True
cursor = conn.cursor()
cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
exists = cursor.fetchone()
if not exists:
    cursor.execute(f"CREATE DATABASE {DB_NAME}")
conn.close()

db = PostgresqlExtDatabase(
    DB_NAME, user="postgres", password="postgres", host="db", port=5432
)


def init_db():
    with db:
        db.create_tables(
            [
                Stock,
                StockDailySummary,
                StockMovingAverage,
                StockRSI,
                StockBollingerBand,
                StockATR,
                StockOBV,
            ],
            safe=True,
        )


class Stock(Model):
    id = AutoField(primary_key=True)
    company_name = CharField()
    industry = TextField()
    symbol = TextField(unique=True)
    series = TextField()
    isin_code = TextField(unique=True)

    class Meta:
        database = db


class StockDailySummary(Model):
    symbol = TextField()
    date = DateTimeField()
    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    prev_close = FloatField()
    volume = FloatField()
    absolute_change = FloatField()
    percentage_change = FloatField(default=0.0)

    class Meta:
        database = db
        unique_together = ("symbol", "date")


class StockMovingAverage(Model):
    symbol = TextField()
    date = DateTimeField()
    moving_average = FloatField()
    period = IntegerField()

    class Meta:
        database = db
        unique_together = ("symbol", "date", "period")


class StockRSI(Model):
    symbol = TextField()
    date = DateTimeField()
    rsi = FloatField()
    period = IntegerField()

    class Meta:
        database = db
        unique_together = ("symbol", "date", "period")


class StockBollingerBand(Model):
    symbol = TextField()
    date = DateTimeField()
    bollinger_upper = FloatField()
    bollinger_lower = FloatField()

    class Meta:
        database = db
        unique_together = ("symbol", "date")


class StockATR(Model):
    symbol = TextField()
    date = DateTimeField()
    atr = FloatField()
    period = IntegerField()

    class Meta:
        database = db
        unique_together = ("symbol", "date", "period")


class StockOBV(Model):
    symbol = TextField()
    date = DateTimeField()
    obv = FloatField()
    period = IntegerField()

    class Meta:
        database = db
        unique_together = ("symbol", "date", "period")
