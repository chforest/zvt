import requests
import json
from datetime import datetime, timedelta
import pandas as pd


def get_vix_k_data():
    """
    数据从investing获取
    1、https://cn.investing.com/indices/volatility-s-p-500查询data-pair-id
    2、查询历史K线
    """
    headers = {'Accept': 'text/html, application/xhtml+xml, image/jxr, */*',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-Hans-CN, zh-Hans; q=0.5',
               'Connection': 'Keep-Alive',
               'Host': 'cn.investing.com',
               'Content-Type':'text/html; charset=UTF-8',
               'Content-Security-Policy':'upgrade-insecure-requests; block-all-mixed-content',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063'}

    response = requests.get(url=r'https://cn.investing.com/indices/volatility-s-p-500', headers=headers)
    if response.status_code == 200:
        content = response.content.decode(encoding='utf8', errors='strict')
        index = content.index('data-pair-id="')
        if index >= 0:
            end = content.index('"', index+len('data-pair-id="'))
            print("start = ", index)
            print("end = ", end)
            pair_id = content[index+len('data-pair-id="'):end]
            print(pair_id)

            # query hist data now
            headers = {
                'Referer': 'https://cn.investing.com/indices/volatility-s-p-500',
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'Keep-Alive',
                'Host': 'cn.investing.com',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063'
            }

            url = 'https://cn.investing.com/common/modules/js_instrument_chart/api/data.php?pair_id={0}&' \
                  'pair_id_for_news={0}&chart_type=candlestick&pair_interval=86400&candle_count={1}&events=yes&' \
                  'volume_series=yes&period=max'.format(pair_id, 500)
            #print(url)
            k_data_response = requests.get(url=url, headers=headers)
            if k_data_response.status_code == 200:
                k_data_content = k_data_response.content.decode(encoding='utf8', errors='strict')
                #print(k_data_content)

                start = k_data_content.index('candles":')
                end = k_data_content.index(',"events')
                k_data = k_data_content[start+len('candles ":')-1: end]
                #print(k_data)
                k_data_list = json.loads(k_data)
                #print(k_data_list)

                columns = [
                    'date',
                    'open',
                    'high',
                    'low',
                    'close',
                ]
                data_list = []
                syms_list = []

                for item in k_data_list:
                    utc = item[0]
                    open = item[1]
                    high = item[2]
                    low = item[3]
                    close = item[4]
                    dt = datetime.utcfromtimestamp(utc/1000)
                    dt = dt + timedelta(hours=8)
                    data_list.append([dt.strftime("%Y-%m-%d"), open, high, low, close])
                df = pd.DataFrame(data_list, columns=columns)
                return df


def get_k_data(code):
    """
    数据从investing获取
    1、https://cn.investing.com/indices/{code}查询data-pair-id
    2、根据pair-id查询历史K线
    """
    headers = {'Accept': 'text/html, application/xhtml+xml, image/jxr, */*',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-Hans-CN, zh-Hans; q=0.5',
               'Connection': 'Keep-Alive',
               'Host': 'cn.investing.com',
               'Content-Type':'text/html; charset=UTF-8',
               'Content-Security-Policy':'upgrade-insecure-requests; block-all-mixed-content',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063'}

    url = "https://cn.investing.com/indices/{0}".format(code)
    print(url)
    response = requests.get(url=url, headers=headers)
    if response.status_code == 200:
        content = response.content.decode(encoding='utf8', errors='strict')
        index = content.index('data-pair-id="')
        if index >= 0:
            end = content.index('"', index+len('data-pair-id="'))
            print("start = ", index)
            print("end = ", end)
            pair_id = content[index+len('data-pair-id="'):end]
            print(pair_id)

            # query hist data now
            headers = {
                'Referer': url,
                'X-Requested-With': 'XMLHttpRequest',
                'Connection': 'Keep-Alive',
                'Host': 'cn.investing.com',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063'
            }

            url = 'https://cn.investing.com/common/modules/js_instrument_chart/api/data.php?pair_id={0}&' \
                  'pair_id_for_news={0}&chart_type=candlestick&pair_interval=86400&candle_count={1}&events=yes&' \
                  'volume_series=yes&period=max'.format(pair_id, 100)
            print(url)
            k_data_response = requests.get(url=url, headers=headers)
            if k_data_response.status_code == 200:
                k_data_content = k_data_response.content.decode(encoding='utf8', errors='strict')
                print(k_data_content)

                start = k_data_content.index('candles":')
                end = k_data_content.index(',"events')
                k_data = k_data_content[start+len('candles ":')-1: end]
                print(k_data)
                k_data_list = json.loads(k_data)
                print(k_data_list)

                columns = [
                    'date',
                    'open',
                    'high',
                    'low',
                    'close',
                ]
                data_list = []
                syms_list = []

                for item in k_data_list:
                    utc = item[0]
                    open = item[1]
                    high = item[2]
                    low = item[3]
                    close = item[4]
                    dt = datetime.utcfromtimestamp(utc/1000)
                    dt = dt + timedelta(hours=8)
                    data_list.append([dt.strftime("%Y-%m-%d"), open, high, low, close])
                df = pd.DataFrame(data_list, columns=columns)
                return df

