# -*- coding: utf-8 -*-
from zvt.utils.time_utils import to_time_str, to_pd_timestamp
from zvt.utils.utils import to_float, to_int
from zvt.api.quote import to_report_period_type
from zvt.domain.misc.holder import TopTenHolder, InstitutionalInvestorHolderOverall
from zvt.recorders.eastmoney.common import EastmoneyTimestampsDataRecorder, get_fc
from datetime import datetime, date, time, timedelta
import requests
import json
import pprint
import pandas as pd
from zvt.utils.utils import chrome_copy_header_to_dict


EASTMONEY_HEADER = chrome_copy_header_to_dict('''
Host: datacenter.eastmoney.com
Origin: https://emdata.eastmoney.com
Referer: https://emdata.eastmoney.com/
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-site
User-Agent: Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Mobile Safari/537.36
''')


class InstitutionalInvestorHolderOverallRecorder(EastmoneyTimestampsDataRecorder):
    provider = 'eastmoney'
    data_schema = InstitutionalInvestorHolderOverall

    url = 'https://datacenter.eastmoney.com/securities/api/data/get?type=RPT_MAIN_ORGHOLD&sty=ALL&source=DataCenter&client=WAP&p={page}&ps={page_size}&sr=&st=&filter=(SECURITY_CODE=%22{security_code}%22)(REPORT_DATE=%27{report_date}%27)&?v=09551304833606751'

    timestamps_fetching_url = 'https://datacenter.eastmoney.com/securities/api/data/get?type=RPT_MAIN_REPORTDATE&sty=ALL&source=DataCenter&client=WAP&p=1&ps=1000&sr=&st=&?v=012429884849719142'
    timestamp_list_path_fields = ['SDGDBGQ', 'ShiDaGuDongBaoGaoQiList']
    timestamp_path_fields = ['BaoGaoQi']

    def after_list(self, list_date: datetime, report_date_str: str):
        report_date = datetime.strptime(report_date_str, "%Y-%m-%d %H:%M:%S")
        return report_date >= list_date

    def init_timestamps(self, entity):
        param = {
            "color": "w",
        }

        response = requests.get(self.timestamps_fetching_url)
        response_dict = json.loads(response.content)
        # pprint.pprint(response_dict)
        timestamp_json_list = response_dict['result']['data']

        list_date = entity.list_date
        timestamps = [x['REPORT_DATE'] for x in timestamp_json_list if self.after_list(list_date, x['REPORT_DATE'])]
        return [to_pd_timestamp(t) for t in timestamps]

    def query_overall_data(self, security_code: str, report_date: str):
        url = self.url.format(page=1, page_size=100, security_code=security_code, report_date=report_date)
        response = requests.get(url, headers=EASTMONEY_HEADER)
        data_dict = json.loads(response.content)

        self.logger.info('code = {}, message = {}'.format(data_dict['code'], data_dict['message']))
        if data_dict['result'] is None:
            return []
        else:
            return data_dict['result']['data']

    def get_data_map(self):
        return {
            "report_period": ("timestamp", to_report_period_type),
            "report_date": ("timestamp", to_pd_timestamp),
            # 持股总数（占流通股）
            "free_shareholding_numbers": ("FREE_SHARES", to_float),
            # 持股比例（占流通股）
            "free_shareholding_ratio": ("FREESHARES_RATIO", to_float),
            # 持股总数
            "total_shareholding_numbers": ("TOTAL_SHARES", to_float),
            # 持股比例
            "total_shareholding_ratio": ("TOTALSHARES_RATIO", to_float),
            # 持股机构家数
            "holder_numbers": ("HOULD_NUM", to_int),
            "organ_type": ("ORG_TYPE", to_int),
            "organ_name": ("ORG_TYPE_NAME"),
        }

    def get_original_time_field(self):
        return 'REPORT_DATE'

    def generate_domain_id(self, entity, original_data):
        the_name = original_data.get("ORG_TYPE_NAME")
        timestamp = original_data[self.get_original_time_field()]
        the_id = "{}_{}_{}".format(entity.id, timestamp, the_name)
        return the_id

    def record(self, entity_item, start, end, size, timestamps):
        if timestamps:
            original_list = []
            count = 0
            for the_timestamp in timestamps:
                self.logger.info(
                    "record {} for entity_id:{},timestamp:{}".format(
                        self.data_schema, entity_item.id, the_timestamp))

                report_date = pd.to_datetime(the_timestamp).strftime('%Y-%m-%d')

                # CALL API to query data
                original_data = self.query_overall_data(entity_item.code, report_date)

                if original_data is not None:
                    original_list += original_data

                count += 1
                if count == self.batch_size:
                    break
            return original_list

        else:
            raise ValueError("Not implemented")


__all__ = ['InstitutionalInvestorHolderOverallRecorder']

if __name__ == '__main__':
    # init_log('top_ten_holder.log')

    InstitutionalInvestorHolderOverallRecorder(sleeping_time=2.).run()

