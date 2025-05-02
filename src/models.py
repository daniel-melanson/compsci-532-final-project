from peewee import *

DB_FILE_PATH = "data/nifty_500_analytics.db"
db = SqliteDatabase(DB_FILE_PATH)


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
    id = AutoField(primary_key=True)
    symbol = TextField()
    date = TextField()
    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    volume = FloatField()
    absolute_change = FloatField()
    percentage_change = FloatField(default=0.0)

    class Meta:
        database = db
        unique_together = ("symbol", "date")
