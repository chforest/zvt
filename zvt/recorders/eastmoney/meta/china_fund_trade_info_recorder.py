import os
import requests
from bs4 import BeautifulSoup
import re


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


if __name__ == '__main__':
    info = get_fund_trade_info('007119')
    print(info)

    print('')
    info = get_fund_trade_info('161005')
    print(info)

    print('')
    info = get_fund_trade_info('110011')
    print(info)

    print('')
    info = get_fund_trade_info('005491')
    print(info)

    print('')
    info = get_fund_trade_info('163118')
    print(info)

    print('')
    info = get_fund_trade_info('510050')
    print(info)

