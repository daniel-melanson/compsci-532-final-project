from peewee import Model, CharField, TextField, SqliteDatabase, FloatField

DB_FILE_PATH = "data/nifty_500_analytics.db"
db = SqliteDatabase(DB_FILE_PATH)

class Stock(Model):
    company_name = CharField()
    industry = TextField()
    symbol = TextField()
    series = TextField()
    isin_code = TextField()

    class Meta:
        database = db
        

class StockDailyChange(Model):
    symbol = TextField()
    date = TextField()
    change = FloatField()
    
    class Meta:
        database = db