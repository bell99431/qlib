import abc
import sys
import datetime
from abc import ABC
from pathlib import Path

import fire
import requests
import pandas as pd
from loguru import logger
from dateutil.tz import tzlocal

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))
from data_collector.base import BaseCollector, BaseNormalize, BaseRun
from data_collector.utils import deco_retry

from pycoingecko import CoinGeckoAPI
from time import mktime
from datetime import datetime as dt
import time


_CG_CRYPTO_SYMBOLS = None


symbol = 'solana'
start = '2022-01-01'
end = '2022-01-07'
from_timestamp = time.mktime( time.strptime(start, "%Y-%m-%d") )
to_timestamp = time.mktime( time.strptime(end, "%Y-%m-%d") )
cg = CoinGeckoAPI()
# data = cg.get_coin_ohlc_by_id(id=symbol, vs_currency="usd", days="max")
# print(data.keys())
# days: 1/7/14/30/90/180/365/max
data = cg.get_coin_market_chart_by_id(id=symbol, vs_currency="usd", days="7", interval="daily")
# data = cg.get_coin_market_chart_range_by_id(id=symbol, vs_currency="usd", from_timestamp=from_timestamp, to_timestamp=to_timestamp)

# 此数据是4小时间隔的!
ohlc = cg.get_coin_ohlc_by_id(id=symbol, vs_currency="usd", days="30", interval="daily")
print(data.keys())
print(data)
# data = pd.DataFrame(data + ohlc)
print(ohlc)
_resp = pd.DataFrame(columns=["date"] + list(data.keys()))
_resp["date"] = [dt.fromtimestamp(mktime(time.localtime(x[0] / 1000))) for x in data["prices"]]
for key in data.keys():
    _resp[key] = [x[1] for x in data[key]]
_resp["date"] = pd.to_datetime(_resp["date"])
_resp["date"] = [x.date() for x in _resp["date"]]
_resp = _resp[(_resp["date"] < pd.to_datetime(end).date()) & (_resp["date"] > pd.to_datetime(start).date())]
if _resp.shape[0] != 0:
    _resp = _resp.reset_index()
if isinstance(_resp, pd.DataFrame):
    print(_resp.keys())
    print(_resp)