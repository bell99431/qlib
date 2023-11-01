# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import abc
from re import I
import sys
import copy
import time
import datetime
import importlib
from abc import ABC
import multiprocessing
from pathlib import Path
from typing import Iterable

import fire
import requests
import numpy as np
import pandas as pd
from loguru import logger
from yahooquery import Ticker
from dateutil.tz import tzlocal

from qlib.tests.data import GetData
from qlib.utils import code_to_fname, fname_to_code, exists_qlib_data
from qlib.constant import REG_CN as REGION_CN

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

from dump_bin import DumpDataUpdate
from data_collector.base import BaseCollector, BaseNormalize, BaseRun, Normalize
from data_collector.utils import (
    deco_retry,
    get_calendar_list,
    get_hs_stock_symbols,
    get_us_stock_symbols,
    get_in_stock_symbols,
    get_br_stock_symbols,
    generate_minutes_calendar_from_daily,
)

# from pycoingecko import CoinGeckoAPI

# INDEX_BENCH_URL = "http://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.{index_code}&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58&klt=101&fqt=0&beg={begin}&end={end}"

# interval = "1d"
# start = "2022-10-01"
# end = "2022-12-01"
# symbol = "1inch-usd"
# aapl = Ticker(symbol, asynchronous=False)
# history = aapl.history(interval=interval, start=start, end=end)
# print( history )
# if isinstance(history, pd.DataFrame):
#     print(history.keys())
#     print( history[0:1] )

# cal = get_calendar_list("CSI300")
# print(cal)

# cg = CoinGeckoAPI()
# resp = pd.DataFrame(cg.get_coins_markets(vs_currency="usd"))
# resp["atl_date"] = resp["atl_date"].astype('datetime64')
# resp['list_date'] = resp["atl_date"].map(lambda x:x.strftime('%Y%m%d'))
# resp.to_csv("test.csv")
# id symbol      name                                              image  current_price  ...        atl  atl_change_percentage                  atl_date                                                roi              last_updated
# 0   bitcoin    btc   Bitcoin  https://assets.coingecko.com/coins/images/1/la...       16475.30  ...  67.810000            24184.42484  2013-07-06T00:00:00.000Z                                               None  2022-11-29T07:05:03.143Z
# print(resp[0:2])
# for index, row in resp.iterrows():
#     symbol = row['symbol'] + '-usd'
#     print(index, symbol)
#     try:
#         _resp = Ticker(symbol, asynchronous=False).history(interval=interval, start=start, end=end)
#         if isinstance(_resp, pd.DataFrame):
#             print( _resp.reset_index() )
#         else:
#             print("error!", _resp)
#     except Exception as e:
#         logger.warning(
#             f"get data error: {symbol}--{start}--{end}"
#             + "Your data request fails. This may be caused by your firewall (e.g. GFW). Please switch your network if you want to access Yahoo! data"
#             + {e}
#         )
    # if index>5:
    #     break


import http.client

conn = http.client.HTTPSConnection("pro-api.coinmarketcap.com")
payload = ''
headers = {
   'Accepts': 'application/json',
   'X-CMC_PRO_API_KEY': 'c07ca245-4521-4867-8af9-31f3fd86407e',
   'User-Agent': 'Apifox/1.0.0 (https://www.apifox.cn)',
   'Accept': '*/*',
   'Host': 'pro-api.coinmarketcap.com',
   'Connection': 'keep-alive'
}
conn.request("GET", "/v1/cryptocurrency/listings/latest?start=1&limit=150&convert=USD", payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))
