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

ENTITY_K_DATA_STATUS_UNKNOWN = 1          # 未知
ENTITY_K_DATA_STATUS_UP_TO_DATE = 2       # 最新
ENTITY_K_DATA_STATUS_OUT_OF_DATE = 3      # 过时

"""
entity_type: stock/etf/index
"""


class TushareBaseKdataRecorder(FixedCycleDataRecorder):

    # 数据来自jq
    provider = 'ts'

    def __init__(self,
                 entity_type='stock',
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
                 level=IntervalLevel.LEVEL_1DAY,
                 kdata_use_begin_time=False,
                 close_hour=15,
                 close_minute=0,
                 one_day_trading_minutes=4 * 60,
                 adjust_type=AdjustType.qfq) -> None:
        level = IntervalLevel(level)
        adjust_type = AdjustType(adjust_type)
        self.entity_type = entity_type
        self.data_schema = get_kdata_schema(entity_type=entity_type, level=level, adjust_type=adjust_type)
        self.ts_trading_level = to_ts_trading_level(level)

        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)
        self.adjust_type = adjust_type
        ts.set_token(zvt_env['tushare_access_token'])

        if entity_type == 'stock':
            self.asset = 'E'
        elif entity_type == 'etf':
            self.asset = 'FD'
        elif entity_type == 'index':
            self.asset = 'I'
        else:
            raise ValueError("Invalid entity type: {0}".format(entity_type))
        # 数据是否为最新，如果是最新数据，则不调用Tushare API，加速同步
        # 判断标准：有一条记录和调用API获取的值一致，则认为当前是最新数据
        self.entity_k_data_status = ENTITY_K_DATA_STATUS_UNKNOWN

        # 交易日历
        pro = ts.pro_api()
        trade_cal_df = pro.query('trade_cal', start_date=to_ts_date(datetime.now()-timedelta(30)), end_date=to_ts_date(datetime.now()), is_open=1)
        self.last_trade_date = datetime.strptime(trade_cal_df.iloc[-1].cal_date, "%Y%m%d")
        print(self.last_trade_date)

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

    def is_up_to_date(self, latest_record_entity, start):
        if latest_record_entity is None:
            return False
        if (latest_record_entity.timestamp == start) and (self.last_trade_date == start):
            return True
        return False

    def get_ts_asset(self):
        if self.entity_type == 'stock':
            return 'E'
        elif self.entity_type == 'index':
            return 'I'
        elif self.entity_type == 'etf':
            return 'FD'
        else:
            raise ValueError("Invalid entity_type: {0}".format(self.entity_type))

    def record(self, entity, start, end, size, timestamps):
        if start is None:
            start = datetime.strptime("20050101", "%Y%m%d")
            size = 10000

        ts_code = to_ts_entity_id(entity)
        adj = to_ts_adj(self.adjust_type)
        max_item_count = 4000
        fetch_count = (size + max_item_count - 1) // max_item_count
        start_date = start
        if end is None:
            end = datetime.now()

        latest_record = self.get_latest_saved_record(entity)
        print('latest_record: ', latest_record)

        # 判断当前状态
        if self.entity_k_data_status == ENTITY_K_DATA_STATUS_UNKNOWN:
            if latest_record is None:
                self.entity_k_data_status = ENTITY_K_DATA_STATUS_OUT_OF_DATE
            else:
                start_date_str = to_ts_date(datetime.now() - timedelta(30))
                end_date_str = to_ts_date(datetime.now())
                df = ts.pro_bar(ts_code=ts_code, asset=self.asset, adj=adj, start_date=start_date_str, end_date=end_date_str)
                if len(df) > 0:
                    if round(latest_record.close, 3) == round(df.close[0], 3):
                        self.entity_k_data_status = ENTITY_K_DATA_STATUS_UP_TO_DATE
                    else:
                        self.entity_k_data_status = ENTITY_K_DATA_STATUS_OUT_OF_DATE
                else:
                    self.entity_k_data_status = ENTITY_K_DATA_STATUS_OUT_OF_DATE

        if self.entity_k_data_status == ENTITY_K_DATA_STATUS_UP_TO_DATE:
            # 已经是最新，则不更新，直接跳过
            if self.is_up_to_date(latest_record, start):
                print("It is up to date!")
                return None

        # tushare限制调用一次返回的数据长度不超过5000，无法一次性获取全部
        # 因为每次调用的时间不一样，导致需要重新计算复权价
        result_df = pd.DataFrame()
        for _ in range(fetch_count):
            end_date = get_coarse_end_date(start_date, max_item_count)
            start_date_str = to_ts_date(start_date)
            end_date_str = to_ts_date(end_date)
            df = ts.pro_bar(ts_code=ts_code, asset=self.asset, adj=adj, start_date=start_date_str, end_date=end_date_str, adjfactor=True)
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

                if self.last_trade_date == latest_date:
                    break
                else:
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


__all__ = ['TushareBaseKdataRecorder']

