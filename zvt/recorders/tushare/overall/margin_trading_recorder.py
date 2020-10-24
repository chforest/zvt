from jqdatapy.api import run_query
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.domain import Index, MarginTradingSummary
from zvt.utils.time_utils import to_time_str
from zvt import zvt_env
import tushare as ts
from datetime import datetime, timedelta
from zvt.recorders.tushare.common import to_ts_trading_level, to_ts_entity_id, to_ts_adj, to_ts_date, get_coarse_end_date


# Tushare编码
# SSE-上海证券交易所
# SZSE-深圳证券交易所

code_map_jq = {
    '000001': 'SSE',    # 上证综指
    '399106': 'SZSE'    # 深圳综指
}

DEFAULT_SIZE = 20000


class MarginTradingSummaryRecorder(TimeSeriesDataRecorder):
    entity_provider = 'exchange'
    entity_schema = Index

    provider = 'ts'
    data_schema = MarginTradingSummary

    def __init__(self, batch_size=10,
                 force_update=False, sleeping_time=1.3, default_size=DEFAULT_SIZE, real_time=False,
                 fix_duplicate_way='add') -> None:
        # 上海A股,深圳市场
        codes = ['000001', '399106']
        super().__init__('index', ['cn'], None, codes, batch_size,
                         force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way)
        ts.set_token(zvt_env['tushare_access_token'])

    def record(self, entity, start, end, size, timestamps):
        ts_code = code_map_jq.get(entity.code)

        if start is None:
            start = datetime.strptime("20050101", "%Y%m%d")
            size = DEFAULT_SIZE
            end = start + timedelta(days=DEFAULT_SIZE)
        elif end is None:
            end = start + timedelta(days=DEFAULT_SIZE)

        start_date = start
        end_date = end
        start_date_str = to_ts_date(start_date)
        end_date_str = to_ts_date(end_date)

        pro = ts.pro_api()
        df = pro.query('margin', start_date=start_date_str, end_date=end_date_str, exchange_id='SSE')
        print(df)

        json_results = []

        for item in df.to_dict(orient='records'):
            result = {
                'provider': self.provider,
                'timestamp': datetime.strptime(item['trade_date'], '%Y%m%d'),
                'name': entity.name,
                'margin_value': item['rzye'],   # 融资余额
                'margin_buy': item['rzmre'],    # 买入额
                'margin_return': item['rzche'],       # 融资偿还额
                'short_value': item['rqye'],    # 融券余额
                'short_volume': item['rqmcl'],  # 卖出量, 股,份,手
                'total_value': item['rzrqye'],  # 融资融券余额，元
                'short_total_value': item['rqyl'],  # 融券余量(股,份,手)
            }

            json_results.append(result)

        if len(json_results) < 100:
            self.one_shot = True

        return json_results

    def get_data_map(self):
        return None


__all__ = ['MarginTradingSummaryRecorder']

if __name__ == '__main__':
    MarginTradingSummaryRecorder(batch_size=30).run()
    df = MarginTradingSummary.query_data(provider='ts', entity_id='index_cn_000001')
    print(df)