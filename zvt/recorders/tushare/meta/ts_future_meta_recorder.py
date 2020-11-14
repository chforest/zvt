# -*- coding: utf-8 -*-
import pandas as pd

from jqdatapy.api import get_all_securities, run_query, get_token
from zvt.api.quote import china_stock_code_to_id, portfolio_relate_stock
from zvt.contract.api import df_to_db, get_entity_exchange, get_entity_code
from zvt.contract.recorder import Recorder, TimeSeriesDataRecorder
from zvt.domain import Future
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str
from zvt import zvt_env
import tushare as ts



columns = ['id', 'entity_id', 'timestamp', 'entity_type', 'exchange', 'code', 'name',  'index', 'list_date', 'end_date']
futures = [
    # id,               entity_id,          timestamp,             entity_type, exchange,  code, name,  index, list-date, end-date
    ['future.cboe.vix', 'future.cboe.vix',  '2001-01-04 08:00:00', 'future',    'cboe',    'vix', 'VIX恐慌指数', 'S&P500', '2001-01-04 08:00:00', None]
]


class TushareFutureRecorder(Recorder):
    data_schema = Future
    provider = 'ts'

    def run(self):
        # 抓取Future列表，固定的方式

        df_future = pd.DataFrame(data=futures, columns=columns)
        df_future['timestamp'] = pd.to_datetime(df_future['timestamp'])
        df_future['list_date'] = pd.to_datetime(df_future['list_date'])

        df_to_db(df_future, data_schema=Future, provider=self.provider, force_update=True)

        self.logger.info("persist future list success")


__all__ = ['TushareFutureRecorder']

if __name__ == '__main__':
    TushareFutureRecorder().run()
