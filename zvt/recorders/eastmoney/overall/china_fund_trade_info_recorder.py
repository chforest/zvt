from jqdatapy.api import run_query
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.domain import Index, FundTradeInfo, Fund
from zvt.utils.time_utils import to_time_str
from zvt.utils.utils import multiple_number
import requests
from bs4 import BeautifulSoup
import re
from _datetime import datetime


def get_fund_trade_info(fund_code: str):
    url = 'http://fund.eastmoney.com/{}.html'.format(fund_code)
    response = requests.get(url)
    if response.status_code != 200:
        return None

    soup = BeautifulSoup(response.content, features="html.parser")
    # print(soup.title.text)
    title_text: str = soup.title.text
    fund_name = title_text[0: title_text.find('(')]

    buy_way_static = soup.findChild(class_='buyWayStatic')
    # print(buy_way_static)

    infos = buy_way_static.findAll(class_='staticCell')
    # print(infos)

    # print('')
    # print('{}, info lines = {}'.format(fund_code, len(infos)))

    money = 999999999
    for info in infos:
        if '不开放购买' in info.text:
            money = 0

    info_count = len(infos)
    for info in infos:
        print(info.text)

    buy_status = None
    buy_info = infos[0].text
    buy_status = buy_info.split(' ')[0]
    print('buy_status: {}'.format(buy_status))

    # 括号内有限购信息
    p1 = re.compile(r'[(](.*?)[)]', re.S)
    extra_info = re.findall(p1, buy_info)
    if len(extra_info) > 0:
        xx = extra_info[0]
        if '万' in xx:
            scale = 10000
        else:
            scale = 1

        # 提取金额
        moneys = re.findall(r"\d+\.?\d*", xx)
        if len(moneys) > 0:
            money = int(float(moneys[0]))*scale
            # print('money: {}'.format(money))

    sell_status = infos[1].text

    return fund_code, fund_name, buy_status, money, sell_status


class FundTradeInfoRecorder(TimeSeriesDataRecorder):
    entity_provider = 'eastmoney'
    entity_schema = Fund

    provider = 'eastmoney'
    data_schema = FundTradeInfo

    def __init__(self, batch_size=10,
                 force_update=False, sleeping_time=5, default_size=2000, real_time=False,
                 fix_duplicate_way='add') -> None:

        codes = ['007119', '161005', '110011', '162605', '163415', '163406', '163402', '163417']
        super().__init__('index', ['cn'], None, codes, batch_size,
                         force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way)

    def init_entities(self):
        super().init_entities()

    def record(self, entity, start, end, size, timestamps):
        info = get_fund_trade_info(entity.code)
        if info is None:
            return None

        df = None
        fund_code, fund_name, buy_status, money, sell_status = info
        json_results = []

        result = {
            'provider': self.provider,
            'timestamp': datetime.now(),
            'name': entity.name,
            'fund_code': fund_code,
            'fund_name': fund_name,
            'buy_status': buy_status,
            'buy_limit': money,
            'sell_status': sell_status
        }

        json_results.append(result)

        if len(json_results) < 100:
            self.one_shot = True

        return json_results

    def get_data_map(self):
        return None


__all__ = ['FundTradeInfoRecorder']

if __name__ == '__main__':
    FundTradeInfoRecorder(batch_size=30, sleeping_time=1.).run()
