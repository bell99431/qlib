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
from binance.spot import Spot
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

_CG_CRYPTO_SYMBOLS = None
client = Spot()

def get_binance_symbols(qlib_data_path: [str, Path] = None) -> list:
    """get crypto symbols in coingecko

    Returns
    -------
        crypto symbols in given exchanges list of coingecko
    """
    global _CG_CRYPTO_SYMBOLS

    @deco_retry
    def _get_coinmarketcap():
        """
            从coinmarket获取数据,其对token是按照交易量进行排序的,更有参考价值.
            如果需要获取币安所有现货数据的话,可以采用币安的数据更优, client.exchange_info(permissions=["SPOT"])
        """
        
        try:
            #limit可以控制数量
            url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?start=1&limit=150&convert=USD"
            payload={}
            headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': 'c07ca245-4521-4867-8af9-31f3fd86407e'
            }

            response = requests.request("GET", url, headers=headers, data=payload)

            # logger.info(f"response.text:{response.text}")
            desired_keys = ['name', 'symbol', 'slug', 'date_added', 'max_supply', 'total_supply', 'last_updated']
            response = json.loads(response.text)
            data = [{key: x[key] for key in desired_keys} for x in response['data']]
            data = pd.DataFrame(data)
            data.rename(columns={'symbol':'ts_code', 'slug':'ename', 'date_added':'list_date'}, inplace=True)
            data = data[data["ts_code"].apply(lambda x: x.startswith("$")==False)]
            data = data.drop_duplicates(['ts_code'])
            resp = data.assign(list_status='L')
        except Exception as e:
            logger.warning(f"request error: {e}")
            raise ValueError("request error")
        try:
            # logger.info(f"response:{resp}")
            # resp.filter(like="USD")
            resp.drop(resp[resp['ts_code'].str.contains('USD')].index, inplace=True)
            # logger.info(f"response2:{resp}")
            _symbols = resp["ts_code"].to_list()
            _symbols = [sub + "-USDT" for sub in _symbols]
        except Exception as e:
            logger.warning(f"request error: {e}")
            raise
        return _symbols

    if _CG_CRYPTO_SYMBOLS is None:
        _all_symbols = _get_coinmarketcap()

        _CG_CRYPTO_SYMBOLS = sorted(set(_all_symbols))
        logger.info(_CG_CRYPTO_SYMBOLS)

    return _CG_CRYPTO_SYMBOLS


class BinanceCollector(BaseCollector):
    def __init__(
        self,
        save_dir: [str, Path],
        start=None,
        end=None,
        interval="1d",
        max_workers=1,
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
        super(BinanceCollector, self).__init__(
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

    @staticmethod
    def get_data_from_remote(symbol, interval, start, end):
        error_msg = f"{symbol}-{interval}-{start}-{end}"
        try:
            if interval != BinanceCollector.INTERVAL_1d:
                raise ValueError(f"cannot support {interval}")
            
            freq = "1d" 
            startTime = int(round(start.timestamp() * 1000))
            endTime = int(round(end.timestamp() * 1000))
            bSymbol = symbol.replace("-", "")
            # 这里得拆分成N份,每份1000条数据!
            day1000 = 3600*24*1000*1000
            curTime = startTime
            _resp = []
            while startTime<=endTime:
                curTime = curTime + day1000
                if curTime>endTime:
                    curTime = endTime
                # print("time", startTime, curTime, endTime)
                _respClip = client.klines(symbol=bSymbol, interval=freq, startTime=startTime, endTime=curTime, limit=1000)
                # print(_respClip)
                _resp.extend(_respClip)
                startTime = curTime + 1

            # _resp = client.klines(symbol=bSymbol, interval=freq, startTime=startTime, endTime=endTime, limit=1000)
            data = pd.DataFrame.from_records(_resp, columns=['date','open', 'high', 'low', 'close', 'volume', 'closeTime', 'tradeVolume', 'count', 'buyVolume', 'buyTradeVolume', 'reserved'])
            data.drop(columns=['closeTime', 'tradeVolume', 'count', 'buyVolume', 'buyTradeVolume', 'reserved'], inplace=True)
            data['symbol'] = symbol
            data['adjclose'] = data['close'] 
            data['factor'] = 1  # = data['adjclose']/data['close']
            # print(data)
            # time.localtime()
            # pd.to_datetime()
            def func(x):
                t = time.localtime(x/1000)
                return time.strftime('%Y-%m-%d', t)
            data['date'] = data['date'].apply(lambda x: func(x))

            data.set_index(['symbol', 'date'], inplace=True)
            if isinstance(data, pd.DataFrame):
                return data.reset_index()
        except Exception as e:
            logger.warning(f"{error_msg}:{e}")

    def get_data(
        self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp
    ) -> [pd.DataFrame]:
        def _get_simple(start_, end_):
            self.sleep()
            _remote_interval = interval
            return self.get_data_from_remote(
                symbol,
                interval=_remote_interval,
                start=start_,
                end=end_,
            )

        if interval == self.INTERVAL_1d:
            _result = _get_simple(start_datetime, end_datetime)
        else:
            raise ValueError(f"cannot support {interval}")
        return _result


class BinanceCollector1d(BinanceCollector, ABC):
    def get_instrument_list(self):
        logger.info("get symbols......")
        symbols = get_binance_symbols()
        logger.info(f"get {len(symbols)} symbols.")
        return symbols

    def normalize_symbol(self, symbol):
        return symbol

    @property
    def _timezone(self):
        return "Asia/Shanghai"


# class BinanceNormalize(BaseNormalize):
#     DAILY_FORMAT = "%Y-%m-%d"

#     @staticmethod
#     def _normalize_(
#         df: pd.DataFrame,
#         calendar_list: list = None,
#         date_field_name: str = "trade_date",
#         symbol_field_name: str = "symbol",
#     ):
#         if df.empty:
#             return df
#         df = df.copy()
#         df.set_index(date_field_name, inplace=True)
#         df.index = pd.to_datetime(df.index)
#         df = df[~df.index.duplicated(keep="first")]
#         if calendar_list is not None:
#             df = df.reindex(
#                 pd.DataFrame(index=calendar_list)
#                 .loc[
#                     pd.Timestamp(df.index.min()).date() : pd.Timestamp(df.index.max()).date()
#                     + pd.Timedelta(hours=23, minutes=59)
#                 ]
#                 .index
#             )
#         df.sort_index(inplace=True)

#         df.index.names = [date_field_name]
#         return df.reset_index()

#     def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
#         df = self._normalize_(df, self._calendar_list, self._date_field_name, self._symbol_field_name)
#         return df


# class BinanceNormalize1d(BinanceNormalize):
#     def _get_calendar_list(self):
#         return None
    
#     def adjusted_price(self, df: pd.DataFrame) -> pd.DataFrame:
#         if df.empty:
#             return df
#         df = df.copy()
#         df.set_index(self._date_field_name, inplace=True)
#         if "adjclose" in df:
#             df["factor"] = df["adjclose"] / df["close"]
#             df["factor"] = df["factor"].fillna(method="ffill")
#         else:
#             df["factor"] = 1
#         for _col in self.COLUMNS:
#             if _col not in df.columns:
#                 continue
#             if _col == "volume":
#                 df[_col] = df[_col] / df["factor"]
#             else:
#                 df[_col] = df[_col] * df["factor"]
#         df.index.names = [self._date_field_name]
#         return df.reset_index()

#     def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
#         df = super(BinanceNormalize1d, self).normalize(df)
#         df = self._manual_adj_data(df)
#         return df

#     def _get_first_close(self, df: pd.DataFrame) -> float:
#         """get first close value

#         Notes
#         -----
#             For incremental updates(append) to Yahoo 1D data, user need to use a close that is not 0 on the first trading day of the existing data
#         """
#         df = df.loc[df["close"].first_valid_index() :]
#         _close = df["close"].iloc[0]
#         return _close

#     def _manual_adj_data(self, df: pd.DataFrame) -> pd.DataFrame:
#         """manual adjust data: All fields (except change) are standardized according to the close of the first day"""
#         if df.empty:
#             return df
#         df = df.copy()
#         df.sort_values(self._date_field_name, inplace=True)
#         df = df.set_index(self._date_field_name)
#         _close = self._get_first_close(df)
#         for _col in df.columns:
#             # NOTE: retain original adjclose, required for incremental updates
#             if _col in [self._symbol_field_name, "adjclose", "change"]:
#                 continue
#             if _col == "volume":
#                 df[_col] = df[_col] * _close
#             else:
#                 df[_col] = df[_col] / _close
#         return df.reset_index()

class BinanceNormalize(BaseNormalize):
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
    def normalize_binance(
        df: pd.DataFrame,
        calendar_list: list = None,
        date_field_name: str = "date",
        symbol_field_name: str = "symbol",
        last_close: float = None,
    ):
        if df.empty:
            return df
        symbol = df.loc[df[symbol_field_name].first_valid_index(), symbol_field_name]
        columns = copy.deepcopy(BinanceNormalize.COLUMNS)
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

        change_series = BinanceNormalize.calc_change(df, last_close)
        # NOTE: The data obtained by Yahoo finance sometimes has exceptions
        # WARNING: If it is normal for a `symbol(exchange)` to differ by a factor of *89* to *111* for consecutive trading days,
        # WARNING: the logic in the following line needs to be modified
        _count = 0
        while True:
            # NOTE: may appear unusual for many days in a row
            change_series = BinanceNormalize.calc_change(df, last_close)
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

        df["change"] = BinanceNormalize.calc_change(df, last_close)

        columns += ["change"]
        df.loc[(df["volume"] <= 0) | np.isnan(df["volume"]), columns] = np.nan

        df[symbol_field_name] = symbol
        df.index.names = [date_field_name]
        return df.reset_index()

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # normalize
        df = self.normalize_binance(df, self._calendar_list, self._date_field_name, self._symbol_field_name)
        # adjusted price
        df = self.adjusted_price(df)
        return df

    @abc.abstractmethod
    def adjusted_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """adjusted price"""
        raise NotImplementedError("rewrite adjusted_price")


class BinanceNormalize1d(BinanceNormalize, ABC):
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
        df = super(BinanceNormalize1d, self).normalize(df)
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
        return f"BinanceCollector{self.interval}"

    @property
    def normalize_class_name(self):
        return f"BinanceNormalize{self.interval}"

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
    # get_binance_symbols()
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
