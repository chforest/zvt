# -*- coding: utf-8 -*-
import pandas as pd

from jqdatapy.api import get_all_securities, run_query, get_token
from zvt.api.quote import china_stock_code_to_id, portfolio_relate_stock
from zvt.contract.api import df_to_db, get_entity_exchange, get_entity_code
from zvt.contract.recorder import Recorder, TimeSeriesDataRecorder
from zvt.domain import Future, Index
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str
from zvt import zvt_env
import tushare as ts



future_columns = ['id', 'entity_id', 'timestamp', 'entity_type', 'exchange', 'code', 'name',  'index', 'list_date', 'end_date']
futures = [
    # id,               entity_id,     timestamp,   entity_type, exchange,  code, name,  index, list-date, end-date
    ['future_cboe_vix', 'future_cboe_vix',  '2001-01-04 08:00:00', 'future',    'cboe',    'vix', 'VIX恐慌指数',
     'S&P500', '2001-01-04 08:00:00', None],

    ['index_global_us-dollar', 'index_global_us-dollar', '2001-01-04 08:00:00', 'index', 'global', 'us-dollar',
     '美元指数期货', None, '2001-01-04 08:00:00', None],

    # 道琼斯30指数期货
    # https://cn.investing.com/indices/us-30-futures
    ['future_global_us-30-futures', 'future_global_us-30-futures', '2001-01-04 08:00:00', 'future', 'global',
     'us-30-futures', '道琼斯30指数期货', 'us-30',
     '2001-01-04 08:00:00', None],

    # 美国标普500指数期货 2020年12月
    # https://cn.investing.com/indices/us-spx-500-futures
    ['future_global_us-spx-500-futures', 'future_global_us-spx-500-futures', '2001-01-04 08:00:00', 'future', 'global',
     'us-spx-500-futures', '标普500指数期货', 'us-spx-500', '2001-01-04 08:00:00', None],

    # 纳斯达克100指数期货 2020年12月
    # https://cn.investing.com/indices/nq-100-futures
    ['future_global_nq-100-futures', 'future_global_nq-100-futures', '2001-01-04 08:00:00', 'future', 'global',
     'nq-100-futures', '纳斯达克100指数期货', 'nq-100', '2001-01-04 08:00:00', None],

    # 标普500 VIX指数期货 2020年12月
    # https://cn.investing.com/indices/us-spx-vix-futures
    ['future_global_us-spx-vix-futures', 'future_global_us-spx-vix-futures', '2001-01-04 08:00:00', 'future', 'global',
     'us-spx-vix-futures', '标普500 VIX指数期货', 'us-spx-vix', '2001-01-04 08:00:00', None],

    # 德国DAX指数期货
    # https://cn.investing.com/indices/germany-30-futures
    ['future_global_germany-30-futures', 'future_global_germany-30-futures', '2001-01-04 08:00:00', 'future', 'global',
     'germany-30-futures', '德国DAX指数期货', 'germany-30', '2001-01-04 08:00:00', None],

    # 法国CAC40指数期货
    # https://cn.investing.com/indices/france-40-futures
    ['future_global_france-40-futures', 'future_global_france-40-futures', '2001-01-04 08:00:00', 'future', 'global',
     'france-40-futures', '法国CAC40指数期货', 'france-40', '2001-01-04 08:00:00', None],

    # 英国富时100指数期货
    # https://cn.investing.com/indices/uk-100-futures
    ['future_global_uk-100-futures', 'future_global_uk-100-futures', '2001-01-04 08:00:00', 'future', 'global',
     'uk-100-futures', '英国富时100指数期货', 'uk-100', '2001-01-04 08:00:00', None],

    # 香港恒生指数期货
    # https://cn.investing.com/indices/hong-kong-40-futures
    ['future_global_hong-kong-40-futures', 'future_global_hong-kong-40-futures', '2001-01-04 08:00:00', 'future', 'global',
     'hong-kong-40-futures', '香港恒生指数期货', 'hong-kong-40', '2001-01-04 08:00:00', None],

    # 中国H股期货
    # https://cn.investing.com/indices/china-h-shares-futures
    ['future_global_china-h-shares-futures', 'future_global_china-h-shares-futures', '2001-01-04 08:00:00', 'future', 'global',
     'china-h-shares-futures', '中国H股期货', 'hong-kong-40', '2001-01-04 08:00:00', None],

    # 沪深300指数期货
    # https://cn.investing.com/indices/csi-300-futures
    ['future_global_csi-300-futures', 'future_global_csi-300-futures', '2001-01-04 08:00:00', 'future', 'global',
     'csi-300-futures', '沪深300指数期货', 'csi-300', '2001-01-04 08:00:00', None],

    # 富时中国A50指数期货
    # https://cn.investing.com/indices/china-a50
    ['future_global_china-a50', 'future_global_china-a50', '2001-01-04 08:00:00', 'future', 'global',
     'china-a50', '富时中国A50指数期货', 'china-a50', '2001-01-04 08:00:00', None],

    # 日经225指数期货
    # https://cn.investing.com/indices/japan-225-futures
    ['future_global_japan-225-futures', 'future_global_japan-225-futures', '2001-01-04 08:00:00', 'future', 'global',
     'japan-225-futures', '日经225指数期货', 'japan-225', '2001-01-04 08:00:00', None],
]


index_columns = ['id', 'entity_id', 'timestamp', 'entity_type', 'exchange', 'code', 'name',  'list_date', 'end_date', 'publisher', 'category', 'base_point']
indexs = [
    # id,           entity_id,      timestamp,         entity_type, exchange,  code, name,  index, list-date, end-date
    # https://cn.investing.com/indices/shanghai-composite
    ['index_global_shanghai-composite', 'index_global_shanghai-composite',  '2001-01-04 08:00:00', 'index',    'global',
     'shanghai-composite', '上证指数', '2001-01-04 08:00:00', None, None, None, None],

    # https://cn.investing.com/indices/szse-component
    ['index_global_szse-component', 'index_global_szse-component', '2001-01-04 08:00:00', 'index',    'global',
     'szse-component', '深证成指', '2001-01-04 08:00:00', None, None, None, None],

    # https://cn.investing.com/indices/ftse-china-a50
    ['index_global_ftse-china-a50', 'index_global_ftse-china-a50', '2001-01-04 08:00:00', 'index', 'global',
     'ftse-china-a50', '富时中国A50指数', '2001-01-04 08:00:00', None, None, None, None],

    # https://cn.investing.com/indices/nasdaq-composite
    ['index_global_nasdaq-composite', 'index_global_nasdaq-composite', '2001-01-04 08:00:00', 'index', 'global',
     'nasdaq-composite', '纳斯达克综合指数', '2001-01-04 08:00:00', None, None, None, None],

    # https://cn.investing.com/indices/us-spx-500
    ['index_global_us-spx-500', 'index_global_us-spx-500', '2001-01-04 08:00:00', 'index', 'global', 'us-spx-500',
     '标准普尔500指数', '2001-01-04 08:00:00', None, None, None, None],

    # https://cn.investing.com/indices/us-30
    ['index_global_us-30', 'index_global_us-30', '2001-01-04 08:00:00', 'index', 'global', 'us-30', '道琼斯工业平均指数',
     '2001-01-04 08:00:00', None, None, None, None],

    # https://cn.investing.com/indices/volatility-s-p-500
    ['index_global_volatility-s-p-500', 'index_global_volatility-s-p-500', '2001-01-04 08:00:00', 'index', 'global',
     'volatility-s-p-500', 'VIX恐慌指数', '2001-01-04 08:00:00', None, None, None, None],

    # https://cn.investing.com/indices/germany-30
    ['index_global_germany-30', 'index_global_germany-30', '2001-01-04 08:00:00', 'index', 'global', 'germany-30',
     '德国DAX30指数', '2001-01-04 08:00:00', None, None, None, None],

    # https://cn.investing.com/indices/uk-100
    ['index_global_uk-100', 'index_global_uk-100', '2001-01-04 08:00:00', 'index', 'global', 'uk-100', '英国富时100指数',
     '2001-01-04 08:00:00', None, None, None, None],

    # https://cn.investing.com/indices/france-40
    ['index_global_france-40', 'index_global_france-40', '2001-01-04 08:00:00', 'index', 'global', 'france-40',
     '法国CAC40指数','2001-01-04 08:00:00', None, None, None, None],

    # https://cn.investing.com/indices/japan-ni225
    ['index_global_japan-ni225', 'index_global_japan-ni225', '2001-01-04 08:00:00', 'index', 'global', 'japan-ni225',
     '日经225指数','2001-01-04 08:00:00', None, None, None, None],
]


class InvestingGlobalIndexRecorder(Recorder):
    data_schema = Index
    provider = 'investing'

    def run(self):
        # 抓取Future列表，固定的方式
        df_index = pd.DataFrame(data=indexs, columns=index_columns)
        df_index['timestamp'] = pd.to_datetime(df_index['timestamp'])
        df_index['list_date'] = pd.to_datetime(df_index['list_date'])

        df_to_db(df_index, data_schema=Index, provider=self.provider, force_update=False)

        self.logger.info("persist investing index list success")


class InvestingGlobalFutureRecorder(Recorder):
    data_schema = Future
    provider = 'investing'

    def run(self):
        # 抓取Future列表，固定的方式
        df_future = pd.DataFrame(data=futures, columns=future_columns)
        df_future['timestamp'] = pd.to_datetime(df_future['timestamp'])
        df_future['list_date'] = pd.to_datetime(df_future['list_date'])

        df_to_db(df_future, data_schema=Future, provider=self.provider, force_update=True)

        self.logger.info("persist investing future list success")


__all__ = ['InvestingGlobalIndexRecorder', 'InvestingGlobalFutureRecorder']


if __name__ == '__main__':
    InvestingGlobalIndexRecorder().run()
    InvestingGlobalFutureRecorder().run()
