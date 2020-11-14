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
from zvt.domain import Future, Future1dKdata
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, now_pd_timestamp, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601
import tushare as ts
from zvt import zvt_env
from zvt.recorders.investing.common import get_vix_k_data


class InvestingFutureKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'ts'
    entity_schema = Future

    # 数据来自investing
    provider = 'investing'

    # 只是为了把recorder注册到data_schema
    data_schema = Future1dKdata

    def __init__(self,
                 exchanges=['cboe'],
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=True,
                 sleeping_time=0,
                 default_size=20000,
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

        super().__init__('future', exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)
        self.adjust_type = adjust_type

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data['timestamp'], level=self.level)

    def record(self, entity, start, end, size, timestamps):
        code = entity.code
        if code != "vix":
            raise NotImplementedError("Code: ".format(code))

        df = get_vix_k_data()
        if pd_is_not_null(df):
            df['name'] = entity.name
            df.rename(columns={'date': 'timestamp'}, inplace=True)

            df['entity_id'] = entity.id
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['provider'] = 'ts'
            df['level'] = self.level.value
            df['code'] = entity.code

            def generate_kdata_id(se):
                if self.level >= IntervalLevel.LEVEL_1DAY:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_DAY))
                else:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

            df['id'] = df[['entity_id', 'timestamp']].apply(generate_kdata_id, axis=1)

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

        return None


__all__ = ['InvestingFutureKdataRecorder']

if __name__ == '__main__':
    InvestingFutureKdataRecorder(sleeping_time=0, real_time=False).run()


