# -*- coding: utf-8 -*-

import time

import pandas as pd
import requests

from zvt.contract import IntervalLevel
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.utils.time_utils import get_year_quarters, is_same_date
from zvt.api.quote import generate_kdata_id
from zvt.domain import Index, Index1dKdata
import tushare as ts
from datetime import timedelta
from zvt import zvt_env


class TushareChinaIndexDayKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'exchange'
    entity_schema = Index

    provider = 'ts'
    data_schema = Index1dKdata
    url = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/{}/type/S.phtml?year={}&jidu={}'

    def __init__(self, entity_type='index', exchanges=['cn'], entity_ids=None, codes=None, batch_size=10,
                 force_update=False, sleeping_time=10, default_size=2000, real_time=False, fix_duplicate_way='add',
                 start_timestamp=None, end_timestamp=None,
                 level=IntervalLevel.LEVEL_1DAY, kdata_use_begin_time=False, close_hour=0, close_minute=0,
                 one_day_trading_minutes=24 * 60) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)
        ts.set_token(zvt_env['tushare_access_token'])
        # no_csi_entities = [item for item in self.entities if item.code[0] == '0' or item.code[0] == '3']
        # self.entities = no_csi_entities

    def get_data_map(self):
        return {}

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity.id, timestamp=original_data['timestamp'], level=self.level)

    def get_entity_exchange(self, entity):
        if entity.code[0] == '0':
            exchange = 'SH'
        elif entity.code[0] == '3':
            exchange = 'SZ'
        elif entity.code[0] == '9':
            exchange = 'CSI'
        else:
            raise ValueError("Not support!")
        return exchange

    def record(self, entity, start, end, size, timestamps):
        pro = ts.pro_api()
        max_item_count = 5000
        count = (size + max_item_count -1)//max_item_count

        result_df = pd.DataFrame()

        start_date_str = start.strftime("%Y%m%d")
        for _ in range(count):
            exchange = self.get_entity_exchange(entity)
            ts_code = "{0}.{1}".format(entity.code, exchange)

            # ts_code\trade_date\close\open\high\low\pre_close\change\pct_chg\vol\amount
            df = pro.index_daily(ts_code=ts_code, start_date=start_date_str)
            if len(df) == 0:
                break

            df['volume'] = df['vol']
            df['turnover'] = df['amount']
            df['name'] = entity.name
            df['level'] = self.level.value
            df['timestamp'] = pd.to_datetime(df['trade_date'])
            df['provider'] = 'ts'

            result_df = pd.concat([result_df, df])
            latest_date = df.timestamp[0]
            next_date_str = (latest_date+timedelta(days=1)).strftime("%Y-%m-%d")
            start_date_str = next_date_str
            self.sleep()

        if len(result_df) > 0:
            result_df = result_df.sort_values(by='timestamp')

        return result_df.to_dict(orient='records')


__all__ = ['TushareChinaIndexDayKdataRecorder']

if __name__ == '__main__':
    TushareChinaIndexDayKdataRecorder(default_size=2000, sleeping_time=1, fix_duplicate_way='ignore').run()
