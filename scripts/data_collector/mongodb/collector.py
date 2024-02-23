## TODO
## complete data because of klines api limit
## complete normalize data!

import abc
import sys
import datetime
from abc import ABC
from pathlib import Path

import fire
import requests
import pandas as pd
# from mongo.spot import Spot
from loguru import logger
from dateutil.tz import tzlocal

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))
from data_collector.base import BaseCollector, BaseNormalize, BaseRun
from data_collector.utils import deco_retry

from time import mktime
from datetime import datetime as dt
import time
import json
from typing import Iterable
import copy
import numpy as np
import pandas as pd
import pymongo
from pymongo import MongoClient
from pymongo.database import Database
import os

_CG_CRYPTO_SYMBOLS = None
DB_CONN:MongoClient
DB:Database # = to_conn[DB_NAME]
DB_NAME = "cmc"
DB_KLINE_NAME = "cmc_klines"
DB_ADDRESS = "mongodb://center-mongodb:27017/"

DOWNLOAD_URL = Path('~/.qlib/stock_data/source/crypto_mongo').expanduser()
CSV_PATH = Path('~/.qlib/stock_data/target/crypto_mongo_list.csv').expanduser()

def get_mongo_symbols(qlib_data_path: [str, Path] = None) -> list:
    """get crypto symbols in mongo

    Returns
    -------
        ['bitcoin','dogecoin']
    """
    global _CG_CRYPTO_SYMBOLS, DB_CONN, DB_NAME

    if _CG_CRYPTO_SYMBOLS is None:
        stock_list = pd.read_csv(CSV_PATH)
        _CG_CRYPTO_SYMBOLS = stock_list['enname'].to_list()
        # print(_CG_CRYPTO_SYMBOLS)
    return _CG_CRYPTO_SYMBOLS


class MongoCollector(BaseCollector):
    def __init__(
        self,
        save_dir: [str, Path],
        start=None,
        end=None,
        interval="1d",
        max_workers=16,
        max_collector_count=2,
        delay=1,  # delay need to be one
        check_data_length: int = None,
        limit_nums: int = None,
    ):
        """

        Parameters
        ----------
        save_dir: str
            crypto save dir
        max_workers: int
            workers, default 4
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0
        interval: str
            freq, value from [1min, 1d], default 1min
        start: str
            start datetime, default None
        end: str
            end datetime, default None
        check_data_length: int
            check data length, if not None and greater than 0, each symbol will be considered complete if its data length is greater than or equal to this value, otherwise it will be fetched again, the maximum number of fetches being (max_collector_count). By default None.
        limit_nums: int
            using for debug, by default None
        """
        # 强制设置多线程处理
        # max_workers = 16
        print(f'init MongoCollector max_workers: {max_workers}')
        self.init_mongo_db()
        super(MongoCollector, self).__init__(
            save_dir=save_dir,
            start=start,
            end=end,
            interval=interval,
            max_workers=max_workers,
            max_collector_count=max_collector_count,
            delay=delay,
            check_data_length=check_data_length,
            limit_nums=limit_nums,
        )

        self.init_datetime()

    def init_datetime(self):
        if self.interval == self.INTERVAL_1min:
            self.start_datetime = max(self.start_datetime, self.DEFAULT_START_DATETIME_1MIN)
        elif self.interval == self.INTERVAL_1d:
            pass
        else:
            raise ValueError(f"interval error: {self.interval}")

        self.start_datetime = self.convert_datetime(self.start_datetime, self._timezone)
        self.end_datetime = self.convert_datetime(self.end_datetime, self._timezone)

    def init_mongo_db(self):
        global DB_CONN, DB, DB_ADDRESS
        env = os.environ.get("DEV_ENV")
        if env and env=='dev':
            DB_ADDRESS = 'mongodb://178.157.57.88:17000/'
        DB_CONN = pymongo.MongoClient(DB_ADDRESS)
        self.db = DB_CONN[DB_KLINE_NAME]
        self.dowload_url = DOWNLOAD_URL

    @staticmethod
    def convert_datetime(dt: [pd.Timestamp, datetime.date, str], timezone):
        try:
            dt = pd.Timestamp(dt, tz=timezone).timestamp()
            dt = pd.Timestamp(dt, tz=tzlocal(), unit="s")
        except ValueError as e:
            pass
        return dt

    @property
    @abc.abstractmethod
    def _timezone(self):
        raise NotImplementedError("rewrite get_timezone")

    def get_data(
        self, symbol: str, interval: str, start: pd.Timestamp, end: pd.Timestamp
    ) -> [pd.DataFrame]:
        '''
        数据获取逻辑
        '''
        # 获取已有数据的最新时间,
        # return None
        error_msg = f"{symbol}-{interval}-{start}-{end}"
        try:
            if interval != MongoCollector.INTERVAL_1d:
                raise ValueError(f"cannot support {interval}")
            
            freq = "1d" 
            start_time = 0
            try:
                old_data = pd.read_csv(self.dowload_url.joinpath(f'{symbol}.csv'))
                if not old_data.empty:
                    date_string = old_data['date'].max()
                    date_obj = datetime.datetime.strptime(date_string, "%Y-%m-%d")
                    start_time = int(round(date_obj.timestamp()))
                    # print(f'{symbol} latest date: {date_string}')
            except Exception as e:
                # print(e)
                pass
            # 返回数据格式:symbol,date,open,high,low,close,volume,adjclose,factor
            data = self.db[f'cc_kline_{symbol}'].find(
                {
                    "interval":freq,
                    "timeOpen":{"$gt":start_time}
                },
                projection={'_id': 0, 'date':'$timeOpen', 'open':1, 'high':1, 'low':1, 'close':1, 'volume':1}
            )
            data = pd.DataFrame(list(data))
            # print(data)

            data['symbol'] = symbol
            data['adjclose'] = data['close'] 
            data['factor'] = 1  # = data['adjclose']/data['close']
            # print(data)
            def func(x):
                t = time.localtime(x)
                return time.strftime('%Y-%m-%d', t)
            data['date'] = data['date'].apply(lambda x: func(x))

            data.set_index(['symbol', 'date'], inplace=True)
            if isinstance(data, pd.DataFrame):
                return data.reset_index()
        except Exception as e:
            logger.warning(f"{error_msg}:{e}")

class MongoCollector1d(MongoCollector, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_instrument_list(self):
        logger.info("get symbols......")
        symbols = get_mongo_symbols()
        logger.info(f"get {len(symbols)} symbols.")
        return symbols

    def normalize_symbol(self, symbol):
        return symbol

    @property
    def _timezone(self):
        return "Asia/Shanghai"

class MongoNormalize(BaseNormalize):
    COLUMNS = ["open", "close", "high", "low", "volume"]
    DAILY_FORMAT = "%Y-%m-%d"

    @staticmethod
    def calc_change(df: pd.DataFrame, last_close: float) -> pd.Series:
        df = df.copy()
        _tmp_series = df["close"].fillna(method="ffill")
        _tmp_shift_series = _tmp_series.shift(1)
        if last_close is not None:
            _tmp_shift_series.iloc[0] = float(last_close)
        change_series = _tmp_series / _tmp_shift_series - 1
        return change_series

    @staticmethod
    def normalize_mongo(
        df: pd.DataFrame,
        calendar_list: list = None,
        date_field_name: str = "date",
        symbol_field_name: str = "symbol",
        last_close: float = None,
    ):
        if df.empty:
            return df
        symbol = df.loc[df[symbol_field_name].first_valid_index(), symbol_field_name]
        columns = copy.deepcopy(MongoNormalize.COLUMNS)
        df = df.copy()
        df.set_index(date_field_name, inplace=True)
        df.index = pd.to_datetime(df.index)
        df = df[~df.index.duplicated(keep="first")]
        if calendar_list is not None:
            df = df.reindex(
                pd.DataFrame(index=calendar_list)
                .loc[
                    pd.Timestamp(df.index.min()).date() : pd.Timestamp(df.index.max()).date()
                    + pd.Timedelta(hours=23, minutes=59)
                ]
                .index
            )
        df.sort_index(inplace=True)
        df.loc[(df["volume"] <= 0) | np.isnan(df["volume"]), list(set(df.columns) - {symbol_field_name})] = np.nan

        change_series = MongoNormalize.calc_change(df, last_close)
        # NOTE: The data obtained by Yahoo finance sometimes has exceptions
        # WARNING: If it is normal for a `symbol(exchange)` to differ by a factor of *89* to *111* for consecutive trading days,
        # WARNING: the logic in the following line needs to be modified
        _count = 0
        while True:
            # NOTE: may appear unusual for many days in a row
            change_series = MongoNormalize.calc_change(df, last_close)
            _mask = (change_series >= 89) & (change_series <= 111)
            if not _mask.any():
                break
            _tmp_cols = ["high", "close", "low", "open", "adjclose"]
            df.loc[_mask, _tmp_cols] = df.loc[_mask, _tmp_cols] / 100
            _count += 1
            if _count >= 10:
                _symbol = df.loc[df[symbol_field_name].first_valid_index()]["symbol"]
                logger.warning(
                    f"{_symbol} `change` is abnormal for {_count} consecutive days, please check the specific data file carefully"
                )

        df["change"] = MongoNormalize.calc_change(df, last_close)

        columns += ["change"]
        df.loc[(df["volume"] <= 0) | np.isnan(df["volume"]), columns] = np.nan

        df[symbol_field_name] = symbol
        df.index.names = [date_field_name]
        return df.reset_index()

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # normalize
        df = self.normalize_mongo(df, self._calendar_list, self._date_field_name, self._symbol_field_name)
        # adjusted price
        df = self.adjusted_price(df)
        return df

    @abc.abstractmethod
    def adjusted_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """adjusted price"""
        raise NotImplementedError("rewrite adjusted_price")


class MongoNormalize1d(MongoNormalize, ABC):
    DAILY_FORMAT = "%Y-%m-%d"

    def _get_calendar_list(self) -> Iterable[pd.Timestamp]:
        return None
    def adjusted_price(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df.set_index(self._date_field_name, inplace=True)
        if "adjclose" in df:
            df["factor"] = df["adjclose"] / df["close"]
            df["factor"] = df["factor"].fillna(method="ffill")
        else:
            df["factor"] = 1
        for _col in self.COLUMNS:
            if _col not in df.columns:
                continue
            if _col == "volume":
                df[_col] = df[_col] / df["factor"]
            else:
                df[_col] = df[_col] * df["factor"]
        df.index.names = [self._date_field_name]
        return df.reset_index()

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super(MongoNormalize1d, self).normalize(df)
        df = self._manual_adj_data(df)
        return df

    def _get_first_close(self, df: pd.DataFrame) -> float:
        """get first close value

        Notes
        -----
            For incremental updates(append) to Yahoo 1D data, user need to use a close that is not 0 on the first trading day of the existing data
        """
        df = df.loc[df["close"].first_valid_index() :]
        _close = df["close"].iloc[0]
        return _close

    def _manual_adj_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """manual adjust data: All fields (except change) are standardized according to the close of the first day"""
        if df.empty:
            return df
        df = df.copy()
        df.sort_values(self._date_field_name, inplace=True)
        df = df.set_index(self._date_field_name)
        _close = self._get_first_close(df)
        for _col in df.columns:
            # NOTE: retain original adjclose, required for incremental updates
            if _col in [self._symbol_field_name, "adjclose", "change"]:
                continue
            if _col == "volume":
                df[_col] = df[_col] * _close
            else:
                df[_col] = df[_col] / _close
        return df.reset_index()


class Run(BaseRun):
    def __init__(self, source_dir=None, normalize_dir=None, max_workers=1, interval="1d"):
        """

        Parameters
        ----------
        source_dir: str
            The directory where the raw data collected from the Internet is saved, default "Path(__file__).parent/source"
        normalize_dir: str
            Directory for normalize data, default "Path(__file__).parent/normalize"
        max_workers: int
            Concurrent number, default is 1
        interval: str
            freq, value from [1min, 1d], default 1d
        """
        super().__init__(source_dir, normalize_dir, max_workers, interval)

    @property
    def collector_class_name(self):
        return f"MongoCollector{self.interval}"

    @property
    def normalize_class_name(self):
        return f"MongoNormalize{self.interval}"

    @property
    def default_base_dir(self) -> [Path, str]:
        return CUR_DIR

    def download_data(
        self,
        max_collector_count=2,
        delay=0,
        start=None,
        end=None,
        check_data_length: int = None,
        limit_nums=None,
    ):
        """download data from Internet

        Parameters
        ----------
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0
        interval: str
            freq, value from [1min, 1d], default 1d, currently only supprot 1d
        start: str
            start datetime, default "2000-01-01"
        end: str
            end datetime, default ``pd.Timestamp(datetime.datetime.now() + pd.Timedelta(days=1))``
        check_data_length: int # if this param useful?
            check data length, if not None and greater than 0, each symbol will be considered complete if its data length is greater than or equal to this value, otherwise it will be fetched again, the maximum number of fetches being (max_collector_count). By default None.
        limit_nums: int
            using for debug, by default None

        Examples
        ---------
            # get daily data
            $ python collector.py download_data --source_dir ~/.qlib/crypto_data/source/1d --start 2015-01-01 --end 2021-11-30 --delay 1 --interval 1d
        """

        super(Run, self).download_data(max_collector_count, delay, start, end, check_data_length, limit_nums)

    def normalize_data(self, date_field_name: str = "date", symbol_field_name: str = "symbol"):
        """normalize data

        Parameters
        ----------
        date_field_name: str
            date field name, default date
        symbol_field_name: str
            symbol field name, default symbol

        Examples
        ---------
            $ python collector.py normalize_data --source_dir ~/.qlib/crypto_data/source/1d --normalize_dir ~/.qlib/crypto_data/source/1d_nor --interval 1d --date_field_name date
        """
        super(Run, self).normalize_data(date_field_name, symbol_field_name)

def hello(name="World"):
    return "Hello %s!" % name

if __name__ == "__main__":
    fire.Fire(Run)
    # get_mongo_symbols()
    # response = client.exchange_info(permissions=["SPOT"])
    # response = client.exchange_info(symbols=['BTCUSDT', 'DOGEUSDT'])
    # logger.info(response)

    # startTime = (datetime.datetime.now()-datetime.timedelta(days=10)).timestamp()
    # startTime = int(round(startTime * 1000))
    # endTime = int(round(datetime.datetime.now().timestamp()*1000))
    # print(startTime, endTime)

    # # limit 500-1000
    # response = client.klines(symbol="BTCUSDT", interval="1d", startTime=startTime, endTime=endTime, limit=500)
    # logger.info(response)
    # data = pd.DataFrame.from_records(response, columns=['date','open', 'high', 'low', 'close', 'volume', 'closeTime', 'tradeVolume', 'count', 'buyVolume', 'buyTradeVolume', 'reserved'])
    # data.drop(columns=['closeTime', 'tradeVolume', 'count', 'buyVolume', 'buyTradeVolume', 'reserved'], inplace=True)
    # data['symbol'] = 'BTCUSDT'
    # data['adjclose'] = data['close'] 
    # print(data)
    # # time.localtime()
    # # pd.to_datetime()
    # def func(x):
    #     t = time.localtime(x/1000)
    #     return time.strftime('%Y-%m-%d', t)
    # data['date'] = data['date'].apply(lambda x: func(x))

    # data.set_index(['symbol', 'date'], inplace=True)
    # print(data)
