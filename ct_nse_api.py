import json
import zipfile
import io
import requests
from dateutil import parser
from datetime import datetime as dt
import pandas as pd

class NSEIndia:
    def __init__(self):
        try:
            self.create_session()
        except:
            pass

    def create_session(self):
        self.session = requests.Session()
        self.session.headers.update({
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"})

        self.session.get('http://nseindia.com')
        self.session_refresh_interval = 300
        self.session_init_time = dt.now()

    def fetch(self, url):
        time_diff = dt.now() - self.session_init_time
        if time_diff.seconds < self.session_refresh_interval:
            print("time diff is ", time_diff.seconds)
            return self.session.get(url).json()
        else:
            print("time diff is ", time_diff.seconds)
            print("re-initing the session because of expiry")
            self.create_session()
            return self.session.get(url).json()

    def fetchContent(self, url):
        time_diff = dt.now() - self.session_init_time
        header = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'referer': 'https://www.nseindia.com/api/chart-databyindex?index=OPTIDXBANKNIFTY25-01-2024CE46000.00',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }
        nse_live = self.session.get("http://nseindia.com", headers=header)
        response = self.session.get(url, headers=header)
        self.create_session()
        return response


    # NSE Option-chain data API section
    def getOptionChainData(self, symbol, indices=False):
        try:
            if not indices:
                url = 'https://www.nseindia.com/api/option-chain-equities?symbol=' + symbol
            else:
                url = 'https://www.nseindia.com/api/option-chain-indices?symbol=' + symbol
        except:
            pass
        data = self.session.get(url, headers=self.headers).json()["records"]["data"]

        df = []
        for i in data:
            for keys, values in i.items():
                if keys == 'CE' or keys == 'PE':
                    info = values
                    info['instrumentType'] = keys
                    df.append(info)
        df1 = pd.DataFrame(df)
        return pd.DataFrame(df1)
