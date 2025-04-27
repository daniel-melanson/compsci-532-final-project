# COMPSCI 532 Final Project

## Overview

A market analytics engine for the [Nifty 500](https://www.nseindia.com/products-services/indices-nifty500-index) index using [Apache Spark](https://spark.apache.org/).

## Setup

This project uses [Pipenv](https://pipenv.pypa.io/en/latest/index.html) for dependency management. Install `pipenv` then run `pipenv install` to create the virtual environment.

## Running

### Data

Prior to computing any analytics, the application will first download the necessary data and populate a SQLite database.

Sector information is downloaded straight from the [NSE website](https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv). Instraday trading data is available on [Kaggle](https://www.kaggle.com/datasets/debashis74017/algo-trading-data-nifty-100-data-with-indicators?select=AADHARHFC_minute.csv).


