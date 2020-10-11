# -*- coding: utf-8 -*-
import argparse
import demjson
import requests
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from jqdatapy.api import get_token, get_bars
from zvt import init_log, zvt_env
from zvt.api import get_kdata, AdjustType
from zvt.api.quote import generate_kdata_id, get_kdata_schema
from zvt.contract import IntervalLevel
from zvt.contract.api import df_to_db
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.recorders.tushare.quotes.ts_kdata_recorder_base import TushareBaseKdataRecorder
from zvt.domain import Stock, StockKdataCommon, Stock1dKdata
from zvt.recorders.tushare.common import to_ts_trading_level, to_ts_entity_id, to_ts_adj, to_ts_date, get_coarse_end_date
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, now_pd_timestamp, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601
import tushare as ts
from zvt import zvt_env
from zvt.domain import Etf, Index, Etf1dKdata
from zvt.recorders.consts import EASTMONEY_ETF_NET_VALUE_HEADER


class TushareChinaEtfKdataRecorder(TushareBaseKdataRecorder):
    entity_provider = 'exchange'
    entity_schema = Etf

    provider = 'ts'

    # 只是为了把recorder注册到data_schema
    data_schema = Etf1dKdata

    def __init__(self,
                 entity_type="etf",
                 exchanges=['sh', 'sz'],
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=True,
                 sleeping_time=0,
                 default_size=2000,
                 real_time=False,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 level=IntervalLevel.LEVEL_1WEEK,
                 kdata_use_begin_time=False,
                 close_hour=15,
                 close_minute=0,
                 one_day_trading_minutes=4 * 60,
                 adjust_type=AdjustType.qfq) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, level,
                         kdata_use_begin_time, close_hour, close_minute, one_day_trading_minutes, adjust_type)


    def on_finish_entity(self, entity):
        kdatas = get_kdata(entity_id=entity.id, level=IntervalLevel.LEVEL_1DAY.value,
                           order=Etf1dKdata.timestamp.asc(),
                           return_type='domain', session=self.session,
                           filters=[Etf1dKdata.cumulative_net_value.is_(None)])

        if kdatas and len(kdatas) > 0:
            start = kdatas[0].timestamp
            end = kdatas[-1].timestamp

            # 从东方财富获取基金累计净值
            df = self.fetch_cumulative_net_value(entity, start, end)

            if df is not None and not df.empty:
                for kdata in kdatas:
                    if kdata.timestamp in df.index:
                        kdata.cumulative_net_value = df.loc[kdata.timestamp, 'LJJZ']
                        kdata.change_pct = df.loc[kdata.timestamp, 'JZZZL']
                self.session.commit()
                self.logger.info(f'{entity.code} - {entity.name}累计净值更新完成...')

    def fetch_cumulative_net_value(self, security_item, start, end) -> pd.DataFrame:
        query_url = 'http://api.fund.eastmoney.com/f10/lsjz?' \
                    'fundCode={}&pageIndex={}&pageSize=200&startDate={}&endDate={}'

        page = 1
        df = pd.DataFrame()
        while True:
            url = query_url.format(security_item.code, page, to_time_str(start), to_time_str(end))

            response = requests.get(url, headers=EASTMONEY_ETF_NET_VALUE_HEADER)
            response_json = demjson.decode(response.text)
            response_df = pd.DataFrame(response_json['Data']['LSJZList'])

            # 最后一页
            if response_df.empty:
                break

            response_df['FSRQ'] = pd.to_datetime(response_df['FSRQ'])
            response_df['JZZZL'] = pd.to_numeric(response_df['JZZZL'], errors='coerce')
            response_df['LJJZ'] = pd.to_numeric(response_df['LJJZ'], errors='coerce')
            response_df = response_df.fillna(0)
            response_df.set_index('FSRQ', inplace=True, drop=True)

            df = pd.concat([df, response_df])
            page += 1

            self.logger.info("fetch_cumulative_net_value, sleep!")
            self.sleep()

        return df


__all__ = ['TushareChinaEtfKdataRecorder']


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--level', help='trading level', default='1d', choices=[item.value for item in IntervalLevel])
    parser.add_argument('--codes', help='codes', default=['516260'], nargs='+')

    args = parser.parse_args()

    level = IntervalLevel(args.level)
    codes = args.codes

    init_log('jq_china_stock_{}_kdata.log'.format(args.level))
    TushareChinaEtfKdataRecorder(level=level, entity_ids=['etf_sh_515260'], sleeping_time=0, codes=[], real_time=False).run()

    kdata = get_kdata(entity_id='etf_sh_515260', provider='ts', limit=20000, order=Etf1dKdata.timestamp.desc(),
                    adjust_type=AdjustType.qfq)
    kdata = kdata[::-1]

    ts.set_token(zvt_env['tushare_access_token'])
    df_ts = ts.pro_bar(ts_code='515260.SH', asset='FD', adj='qfq', start_date='19910403', end_date='20201111')
    print('compare now!')

    all_match = True
    min_len = min( len(kdata), len(df_ts))
    print(min_len)
    for i in range(min_len):
        import math
        if (not np.isnan(df_ts.iloc[i].close)) and not math.isclose(kdata.iloc[i].close, df_ts.iloc[i].close, rel_tol=1e-03, abs_tol=0.001):
            all_match = False
            break
    print("all_match = {0}".format(all_match))


