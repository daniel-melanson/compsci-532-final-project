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
        

class StockDailyChange(Model):
    id = AutoField(primary_key=True)
    symbol = TextField(unique=True)
    date = TextField()
    change = FloatField()
    
    class Meta:
        database = db