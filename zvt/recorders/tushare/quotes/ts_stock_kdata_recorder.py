# -*- coding: utf-8 -*-
import argparse

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
from zvt.domain import Stock, StockKdataCommon, Stock1dKdata
from zvt.recorders.tushare.common import to_ts_trading_level, to_ts_entity_id, to_ts_adj, to_ts_date, get_coarse_end_date
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, now_pd_timestamp, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601
import tushare as ts
from zvt import zvt_env


class TushareChinaStockKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'exchange'
    entity_schema = Stock

    # 数据来自jq
    provider = 'ts'

    # 只是为了把recorder注册到data_schema
    data_schema = StockKdataCommon

    def __init__(self,
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
        level = IntervalLevel(level)
        adjust_type = AdjustType(adjust_type)
        self.data_schema = get_kdata_schema(entity_type='stock', level=level, adjust_type=adjust_type)
        self.ts_trading_level = to_ts_trading_level(level)

        super().__init__('stock', exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)
        self.adjust_type = adjust_type
        ts.set_token(zvt_env['tushare_access_token'])


    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data['timestamp'], level=self.level)

    def recompute_qfq(self, entity, qfq_factor, last_timestamp):
        # 重新计算前复权数据
        if qfq_factor != 0:
            kdatas = get_kdata(provider=self.provider, entity_id=entity.id, level=self.level.value,
                               order=self.data_schema.timestamp.asc(),
                               return_type='domain',
                               session=self.session,
                               filters=[self.data_schema.timestamp < last_timestamp])
            if kdatas:
                self.logger.info('recomputing {} qfq kdata,factor is:{}'.format(entity.code, qfq_factor))
                for kdata in kdatas:
                    kdata.open = round(kdata.open * qfq_factor, 5)
                    kdata.close = round(kdata.close * qfq_factor, 5)
                    kdata.high = round(kdata.high * qfq_factor, 5)
                    kdata.low = round(kdata.low * qfq_factor, 5)
                self.session.add_all(kdatas)
                self.session.commit()

    def record(self, entity, start, end, size, timestamps):
        ts_code = to_ts_entity_id(entity)
        adj = to_ts_adj(self.adjust_type)
        max_item_count = 4000
        fetch_count = (size + max_item_count - 1) // max_item_count
        start_date = start
        if end is None:
            end = datetime.now()

        latest_record = self.get_latest_saved_record(entity)
        if (latest_record is not None) and (latest_record.timestamp == start):
            return None

        # tushare限制调用一次返回的数据长度不超过5000，无法一次性获取全部
        # 因为每次调用的时间不一样，导致需要重新计算复权价
        result_df = pd.DataFrame()
        for _ in range(fetch_count):
            end_date = get_coarse_end_date(start_date, max_item_count)
            start_date_str = to_ts_date(start_date)
            end_date_str = to_ts_date(end_date)
            df = ts.pro_bar(ts_code=ts_code, asset='E', adj=adj, start_date=start_date_str, end_date=end_date_str, adjfactor=True)
            if pd_is_not_null(df):
                if pd_is_not_null(result_df):
                    # 调整复权价格: 相同时间的close不同，表明前复权需要重新计算
                    current_bottom = df.iloc[-1]
                    prev_top = result_df.iloc[0]
                    assert current_bottom.trade_date == prev_top.trade_date
                    new_close = current_bottom.close
                    old_close = prev_top.close

                    if self.adjust_type == AdjustType.qfq:
                        if round(old_close, 5) != round(new_close, 5):
                            qfq_factor = new_close / old_close

                            # open high low close pre_close change
                            result_df['open'] = result_df['open'].apply(lambda x: round(x*qfq_factor, 5))
                            result_df['close'] = result_df['close'].apply(lambda x: round(x * qfq_factor, 5))
                            result_df['high'] = result_df['high'].apply(lambda x: round(x * qfq_factor, 5))
                            result_df['low'] = result_df['low'].apply(lambda x: round(x * qfq_factor, 5))
                            result_df['pre_close'] = result_df['pre_close'].apply(lambda x: round(x * qfq_factor, 5))
                            result_df['change'] = result_df['change'].apply(lambda x: round(x * qfq_factor, 5))

                        # 删除df最后重复的一行
                        df.drop(index=[len(df)-1], inplace=True)
                        if len(df) == 0:
                            break
                    else:
                        assert False

                    result_df = pd.concat([df, result_df])
                else:
                    result_df = df

                latest_date = datetime.strptime(df.trade_date[0], "%Y%m%d")
                start_date = latest_date  # 保留一天
                # start_date = (latest_date + timedelta(days=1))
                self.sleep()
            else:
                break

        df = result_df
        if pd_is_not_null(df):
            df['name'] = entity.name
            df.rename(columns={'vol': 'volume', 'amount': 'turnover', 'trade_date': 'timestamp'}, inplace=True)

            df['entity_id'] = entity.id
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['provider'] = 'ts'
            df['level'] = self.level.value
            df['code'] = entity.code

            # 判断是否需要重新计算之前保存的前复权数据
            if self.adjust_type == AdjustType.qfq:
                check_df = df.head(1)
                check_date = check_df['timestamp'][0]
                current_df = get_kdata(entity_id=entity.id, provider=self.provider, start_timestamp=check_date,
                                       end_timestamp=check_date, limit=1, level=self.level,
                                       adjust_type=self.adjust_type)
                if pd_is_not_null(current_df):
                    old = current_df.iloc[0, :]['close']
                    new = check_df['close'][0]
                    # 相同时间的close不同，表明前复权需要重新计算
                    if round(old, 5) != round(new, 5):
                        qfq_factor = new / old
                        last_timestamp = pd.Timestamp(check_date)
                        self.recompute_qfq(entity, qfq_factor=qfq_factor, last_timestamp=last_timestamp)

            def generate_kdata_id(se):
                if self.level >= IntervalLevel.LEVEL_1DAY:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_DAY))
                else:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

            df['id'] = df[['entity_id', 'timestamp']].apply(generate_kdata_id, axis=1)

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

        return None


__all__ = ['TushareChinaStockKdataRecorder']

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--level', help='trading level', default='1d', choices=[item.value for item in IntervalLevel])
    parser.add_argument('--codes', help='codes', default=['000001'], nargs='+')

    args = parser.parse_args()

    level = IntervalLevel(args.level)
    codes = args.codes

    init_log('jq_china_stock_{}_kdata.log'.format(args.level))
    TushareChinaStockKdataRecorder(level=level, sleeping_time=0, codes=codes, real_time=False).run()

    kdata = get_kdata(entity_id='stock_sz_000001', provider='ts', limit=20000, order=Stock1dKdata.timestamp.desc(),
                    adjust_type=AdjustType.qfq)
    kdata = kdata[::-1]

    ts.set_token(zvt_env['tushare_access_token'])
    df_ts = ts.pro_bar(ts_code='000001.SZ', asset='E', adj='qfq', start_date='19910403', end_date='20200930')
    print('compare now!')

    all_match = True
    min_len = min( len(kdata), len(df_ts))
    for i in range(min_len):
        import math
        if (not np.isnan(df_ts.iloc[i].close)) and not math.isclose(kdata.iloc[i].close, df_ts.iloc[i].close, rel_tol=1e-03, abs_tol=0.001):
            all_match = False
            break
    print("all_match = {0}".format(all_match))


