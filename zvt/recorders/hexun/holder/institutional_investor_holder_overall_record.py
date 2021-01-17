# -*- coding: utf-8 -*-
from zvt.utils.time_utils import to_time_str, to_pd_timestamp
from zvt.utils.utils import to_float, to_int
from zvt.api.quote import to_report_period_type
from zvt.domain.misc.holder import TopTenHolder, InstitutionalInvestorHolderOverall
from zvt.recorders.hexun.common import HexunTimestampsDataRecorder
from datetime import datetime, date, time, timedelta
import requests
from zvt.utils.utils import chrome_copy_header_to_dict
import json
import pprint
import demjson
import re
import hjson
import pandas as pd


"""
HEXUN的数据不准确，放弃，改用东财
"""


DEFAULT_HEXUN_HEADER = chrome_copy_header_to_dict('''
Host: stockdata.stock.hexun.com
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36
Accept: */*
Referer: http://www.sse.com.cn/assortment/fund/etf/list/
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9
Cookie: yfx_c_g_u_id_10000042=_ck19062609443812815766114343798; VISITED_COMPANY_CODE=%5B%22510300%22%5D; VISITED_FUND_CODE=%5B%22510300%22%5D; VISITED_MENU=%5B%228307%22%2C%228823%22%2C%228547%22%2C%228556%22%2C%228549%22%2C%2210848%22%2C%228550%22%5D; yfx_f_l_v_t_10000042=f_t_1561513478278__r_t_1561692626758__v_t_1561695738302__r_c_1
Connection: keep-alive
''')

class InstitutionalInvestorHolderOverallRecorder(HexunTimestampsDataRecorder):
    provider = 'hexun'
    data_schema = InstitutionalInvestorHolderOverall

    url = 'https://emh5.eastmoney.com/api/GuBenGuDong/GetShiDaGuDong'
    path_fields = ['ShiDaGuDongList']

    timestamps_fetching_url = 'https://emh5.eastmoney.com/api/GuBenGuDong/GetFirstRequest2Data'
    timestamp_list_path_fields = ['SDGDBGQ', 'ShiDaGuDongBaoGaoQiList']
    timestamp_path_fields = ['BaoGaoQi']

    # 读取每个报告期的数据，并缓存，从2005年开始
    hexun_data_dict = dict()
    is_init_success = False
    data_url = 'http://stockdata.stock.hexun.com/jgcc/Data/outdata/jgcgDetailData.ashx?type=2&count={0}&date={1}&stateType=null&titType=null&page=1&callback=hxbase_json1'
    report_start_year = 2005
    report_count = 50000

    def init_timestamps(self, entity):
        param = {
            "color": "w",
        }

        # 根据entity的list_date计算开始timestamp
        list_date = entity.list_date
        capture_start_dt = date(self.report_start_year, 1, 1)
        if list_date.date() < capture_start_dt:
            start_dt = capture_start_dt
        else:
            start_dt = list_date.date()

        current_dt = datetime.now()
        timestamps = []
        for year in range(start_dt.year, current_dt.year+1):
            season1 = date(year, 3, 31)
            season2 = date(year, 6, 30)
            season3 = date(year, 9, 30)
            season4 = date(year, 12, 31)

            seasons = [season1, season2, season3, season4]

            for season in seasons:
                if season < current_dt.date():
                    timestamps.append(season)

        return [to_pd_timestamp(t) for t in timestamps]

    def init_data(self):

        current_dt = datetime.now()
        for year in range(self.report_start_year, current_dt.year+1):
            season1 = date(year, 3, 31)
            season2 = date(year, 6, 30)
            season3 = date(year, 9, 30)
            season4 = date(year, 12, 31)

            seasons = [season1, season2, season3, season4]

            for season in seasons:
                if season < current_dt.date():
                    report_period = season.strftime("%Y-%m-%d")
                    url = self.data_url.format(self.report_count, report_period)
                    print(url)

                    resp = requests.get(url, headers=DEFAULT_HEXUN_HEADER)
                    # print(resp)

                    length = len(resp.text)
                    prefix_length = len('hxbase_json1(')
                    json_text = resp.text[prefix_length:length-1]

                    data = hjson.loads(json_text)
                    # json_text_patched = re.sub('([{,:])(\w+)([},:])', '\\1\"\\2\"\\3', json_text)
                    # json_text_patched = json_text_patched.replace("'", '"')
                    # data = demjson.decode(json_text)
                    # pprint.pprint(data)

                    """
                    {
                       'AShares': '7.37%',
                       'AddOptional': '<img alt=""  onclick="addIStock(this)" '
                                      'code="600027"  mType="1"  src="img/icon_03.gif"/>',
                       'ForSeason': '--',
                       'ForYear1': '--',
                       'ForYear2': '--',
                       'Industry': '电力（III）',
                       'IndustryLink': 'hy-7530.shtml',
                       'OrgChange': '--',
                       'OrgNum': '6',
                       'OrgNumLink': 'ggccDetail.aspx?code=600027',
                       'StockName': '华电国际',
                       'StockNameLink': 's600027.shtml',
                       'TotalHoldings': '2,094.63',
                       'TotalMarketValue': '3,931.65',
                       'TotalStock': '<img alt=""  src="img/icon_08.gif"/>',
                       'TotalStockLink': 'http://stockdata.stock.hexun.com/600027.shtml',
                       'alt': '600027',
                       'hyAlt': '电力（III）'},
                    """
                    self.hexun_data_dict[report_period] = data
            self.is_init_success = True

    def get_data_map(self):
        return {
            "report_period": ("timestamp", to_report_period_type),
            "report_date": ("timestamp", to_pd_timestamp),
            # 持股总数
            "shareholding_numbers": ("TotalHoldings", to_float),
            # 持股比例（占流通股）
            "shareholding_ratio": ("AShares", to_float),
            # 持股机构家数
            "holder_numbers": ("OrgNum", to_int),
        }

    def generate_request_param(self, security_item, start, end, size, timestamp):
        return {"color": "w",
                "BaoGaoQi": to_time_str(timestamp)
                }

    def generate_domain_id(self, entity, original_data):
        the_name = original_data.get("GuDongMingCheng")
        timestamp = original_data[self.get_original_time_field()]
        the_id = "{}_{}_{}".format(entity.id, timestamp, the_name)
        return the_id

    """
    如果当前时间可能有数据也可能没有，则返回None，否则返回一个空Item表示没有机构持股
    """

    def has_report(self, the_timestamp, entity_item):
        # 如果当前是最后一个报告期，则认为可能没有，例如20201231，则是当前最后一个报告期，可能没有数据
        dt = pd.to_datetime(the_timestamp)
        timestamps = self.init_timestamps(entity_item)
        return the_timestamp < timestamps[-1]

    def generate_none_holder_item(self, entity_item, the_timestamp):

        if self.has_report(the_timestamp, entity_item):
            data = {
                'OrgNum': 0,
                'TotalHoldings': 0.0,
                'AShares': 0.0,
                'alt': entity_item.code,
                'StockName': entity_item.name,
                'timestamp': the_timestamp,
            }
        else:
            data = None
        return data

    def get_original_data(self, entity_item, data, the_timestamp):
        if data is None or 'list' not in data:
            return self.generate_none_holder_item(entity_item, the_timestamp)

        the_list = data['list']
        code = entity_item.code

        for item in the_list:
            if item['alt'] == code:
                item['timestamp'] = the_timestamp
                return item

        return self.generate_none_holder_item(entity_item, the_timestamp)

    def record(self, entity_item, start, end, size, timestamps):
        if timestamps:
            original_list = []
            count = 0
            for the_timestamp in timestamps:
                self.logger.info(
                    "record {} for entity_id:{},timestamp:{}".format(
                        self.data_schema, entity_item.id, the_timestamp))

                key = pd.to_datetime(the_timestamp).strftime('%Y-%m-%d')
                data = self.hexun_data_dict.get(key, None)
                # pprint.pprint(data)

                orignal_data = self.get_original_data(entity_item, data, the_timestamp)

                if orignal_data is not None:
                    original_list.append(orignal_data)

                count += 1
                if count == self.batch_size:
                    break
            return original_list

        else:
            raise ValueError("Not implemented")


__all__ = ['InstitutionalInvestorHolderOverallRecorder']


if __name__ == '__main__':
    # init_log('top_ten_holder.log')
    df = InstitutionalInvestorHolderOverall.query_data(provider="hexun", entity_id='stock_sz_000001')
    print(df)

    # recorder = InstitutionalInvestorHolderOverallRecorder(sleeping_time=0.1)
    # recorder.init_data()
    # recorder.run()


