from jqdatapy.api import run_query
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.domain import Index, Stock, Fund, HKHold
from zvt.utils.time_utils import to_time_str
from zvt.utils.utils import multiple_number
import requests
from bs4 import BeautifulSoup
import re
from _datetime import datetime, date, timedelta
from zvt import zvt_env
import tushare as ts
import math
import pandas as pd
from zvt.recorders.tushare.common import to_ts_trading_level, to_ts_entity_id, to_ts_adj, to_ts_date, get_coarse_end_date
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, now_pd_timestamp, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601
from zvt.contract.api import df_to_db


"""
港股通、深股通、沪股通持股明细数据

沪港通开通时间是2014年11月17日、深港通开通时间是2016年12月5日
"""


class HKHoldRecorder(TimeSeriesDataRecorder):
    entity_provider = 'exchange'
    entity_schema = Stock

    provider = 'ts'
    data_schema = HKHold

    hgt_launch_date = date(2014, 11, 17)

    def init_entities(self):
        super().init_entities()
        ts.set_token(zvt_env['tushare_access_token'])

    def record(self, entity, start, end, size, timestamps):
        pro = ts.pro_api()
        ts_code = to_ts_entity_id(entity)

        start = start.date()
        if start < self.hgt_launch_date:
            start = self.hgt_launch_date
            pass
        else:
            pass

        # 循环获取, tushare限流，单次最大3800
        max_fetch_count = 3800
        start_date = start

        df_list = []
        for i in range(math.ceil(size/max_fetch_count)):
            fetch_start_date = start_date + timedelta(days=i*max_fetch_count)
            fetch_end_date = start_date + timedelta(days=((i+1)*max_fetch_count-1))

            self.sleep()

            df = pro.hk_hold(ts_code=ts_code, start_date=fetch_start_date.strftime("%Y%m%d"), end_date=fetch_end_date.strftime("%Y%m%d"))
            df_list.append(df)
            print(df)
            print(i)
            pass

        if len(df_list) > 0:
            df_all = pd.concat(df_list)
        else:
            df_all = None

        if pd_is_not_null(df_all):
            df.rename(columns={'vol': 'hold_volume', 'code': 'original_code', 'trade_date': 'timestamp', 'ratio': 'hold_ratio'}, inplace=True)
            df['name'] = entity.name
            df['entity_id'] = entity.id
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['provider'] = 'ts'
            df['code'] = entity.code

            def generate_id(se):
                return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

            df['id'] = df[['entity_id', 'timestamp']].apply(generate_id, axis=1)
        pass

        df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

        return None


__all__ = ['HKHoldRecorder']


if __name__ == '__main__':
    print('')
    pro = ts.pro_api()
    df = pro.hk_hold(ts_code='600000.SH')

    HKHoldRecorder(batch_size=3800, sleeping_time=0.7).run()