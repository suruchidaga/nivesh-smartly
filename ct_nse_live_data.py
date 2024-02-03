"""
Retrieve nse information from fno trading website
"""

from nsepython import *
from pytz import timezone
import time
import re
import json
import sys
import traceback


import pandas as pd
from decimal import Decimal
import requests
import pickle
import numpy as np
#from nsetools import Nse
from sqlalchemy import create_engine
from ct_nse_api import *
from datetime import datetime as dt
from datetime import timedelta
import zipfile
from io import BytesIO, StringIO

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SHEETS_READ_WRITE_SCOPE = 'https://www.googleapis.com/auth/spreadsheets'
SCOPES = [SHEETS_READ_WRITE_SCOPE]
nse = NSEIndia()
requestURL = ''
support = {}
supportPCR = {}
resistance = {}
resistancePCR = {}
underlyingValue = {}
nseGainers = []
nseLosers = []
masterList = pd.DataFrame()

liveMarketKeys = ['NIFTY 50', 'NIFTY NEXT 50', 'NIFTY MIDCAP 50', 'NIFTY MIDCAP 100', 'NIFTY MIDCAP 150',
                    'NIFTY SMALLCAP 50', 'NIFTY SMALLCAP 100', 'NIFTY SMALLCAP 250', 'NIFTY MIDSMALLCAP 400',
                    'NIFTY 100', 'NIFTY 200', 'NIFTY500 MULTICAP 50:25:25', 'NIFTY LARGEMIDCAP 250',
                    'NIFTY COMMODITIES', 'NIFTY INDIA CONSUMPTION', 'NIFTY CPSE', 'NIFTY INFRASTRUCTURE', 'NIFTY MNC',
                    'NIFTY GROWTH SECTORS 15', 'NIFTY PSE', 'NIFTY SERVICES SECTOR', 'NIFTY100 LIQUID 15',
                    'NIFTY MIDCAP LIQUID 15',
                    'NIFTY DIVIDEND OPPORTUNITIES 50', 'NIFTY50 VALUE 20', 'NIFTY100 QUALITY 30',
                    'NIFTY50 EQUAL WEIGHT',
                    'NIFTY100 EQUAL WEIGHT', 'NIFTY100 LOW VOLATILITY 30', 'NIFTY ALPHA 50', 'NIFTY200 QUALITY 30',
                    'NIFTY ALPHA LOW-VOLATILITY 30', 'NIFTY200 MOMENTUM 30', 'Securities in F&O', 'Permitted to Trade']

liveSectoralMarketKeys = ['NIFTY AUTO',
                    'NIFTY BANK', 'NIFTY ENERGY', 'NIFTY FINANCIAL SERVICES', 'NIFTY FINANCIAL SERVICES 25/50',
                    'NIFTY FMCG', 'NIFTY IT', 'NIFTY MEDIA', 'NIFTY METAL', 'NIFTY PHARMA', 'NIFTY PSU BANK',
                    'NIFTY REALTY',
                    'NIFTY PRIVATE BANK', 'NIFTY HEALTHCARE INDEX', 'NIFTY CONSUMER DURABLES', 'NIFTY OIL & GAS']

# Credentials to database connection
hostname="localhost"
dbname="research"
uname="root"
pwd="root"

# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))


def bhav_copy_with_delivery():
    """
    get the NSE bhav copy with delivery data as per the traded date
    :param trade_date: eg:'01-06-2023'
    :return: pandas data frame
    """
    tradeDate = dt.now().date() - timedelta(days=5)
    print(tradeDate.strftime('%d-%m-%Y'))
    use_date = tradeDate.strftime('%d%m%Y')
    url = f'https://archives.nseindia.com/products/content/sec_bhavdata_full_{use_date}.csv'
    print(url)
    request_bhav = nse.fetchContent(url)
    print(request_bhav.status_code)
    stockDetails = pd.DataFrame()
    if request_bhav.status_code == 200:
        stockDetails = pd.read_csv(StringIO(request_bhav.text), sep=', ', engine='python')
        stockDetails = stockDetails[stockDetails["SERIES"].isin(['EQ', 'BE'])]
        stockDetails = stockDetails.rename(columns={'DATE1': 'TRADE_DATE'})
    elif request_bhav.status_code == 403:
        raise FileNotFoundError(f' Data not found, change the date...')
    stockDetails = stockDetails.replace(np.nan, 0)
    print(list(stockDetails.columns))
    return stockDetails

if __name__ == "__main__":
    today = dt.now().date()
    now_utc = dt.now(timezone('UTC'))
    dateInYYYYMMDD = today.strftime('%Y%m%d')
    timestamp_UTC = time.mktime(now_utc.timetuple()) #datetime.timestamp(now_utc)

    # stockDetails = bhav_copy_equities()
    # appendToGoogleSheet("1vHN8guyO48u2FjbuOB0EwPPMm9JqKFbpGZwhy5Uja50", "bhav_copy", stockDetails)

    stockDetails = bhav_copy_with_delivery()
    stockDetails.to_sql('CT_NSE_BHAV'.lower(), engine, index=False, if_exists="append")