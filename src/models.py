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
        db.create_tables([Stock, StockDailySummary, StockMovingAverage], safe=True)

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


class StockMovingAverage(Model):
    id = AutoField(primary_key=True)
    symbol = TextField()
    date = TextField()
    moving_average = FloatField()
    period = IntegerField()

    class Meta:
        database = db
        unique_together = ("symbol", "date", "period")