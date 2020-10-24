from jqdatapy.api import run_query
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.domain import Index, HSGCrossMarketSummary
from zvt.utils.time_utils import to_time_str
from zvt.utils.utils import multiple_number
from zvt import zvt_env
import tushare as ts
from datetime import datetime, timedelta
from zvt.recorders.tushare.common import to_ts_trading_level, to_ts_entity_id, to_ts_adj, to_ts_date, get_coarse_end_date


DEFAULT_SIZE = 300


class HSGCrossMarketSummaryRecorder(TimeSeriesDataRecorder):
    entity_provider = 'exchange'
    entity_schema = Index

    provider = 'ts'
    data_schema = HSGCrossMarketSummary

    def __init__(self, batch_size=10,
                 force_update=False, sleeping_time=0, default_size=DEFAULT_SIZE, real_time=False,
                 fix_duplicate_way='add') -> None:

        # 聚宽编码
        # 市场通编码	市场通名称
        # 310001	沪股通
        # 310002	深股通
        # 310003	港股通（沪）
        # 310004	港股通（深）

        codes = ['310001', '310002', '310003', '310004']
        super().__init__('index', ['cn'], None, codes, batch_size,
                         force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way)

    def init_entities(self):
        super().init_entities()

    def record(self, entity, start, end, size, timestamps):
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
        df = pro.query('moneyflow_hsgt', start_date=start_date_str, end_date=end_date_str)
        print(df)

        json_results = []

        for item in df.to_dict(orient='records'):
            result = {
                'provider': self.provider,
                'timestamp': datetime.strptime(item['trade_date'], '%Y%m%d'),
                'name': entity.name,
                'ggt_ss_amount': item['ggt_ss'],
                'ggt_sz_amount': item['ggt_sz'],
                'hgt_amount': multiple_number(item['hgt'], 1000000),
                'sgt_amount': multiple_number(item['sgt'], 1000000),
                'north_total_amount': multiple_number(item['north_money'], 1000000),
                'south_total_amount': multiple_number(item['south_money'], 1000000)
            }

            json_results.append(result)

        if len(json_results) < 100:
            self.one_shot = True

        return json_results

    def get_data_map(self):
        return None


__all__ = ['HSGCrossMarketSummaryRecorder']

if __name__ == '__main__':
    HSGCrossMarketSummaryRecorder(batch_size=30).run()
