# -*- coding: utf-8 -*-
import json

import demjson
import pandas as pd
import requests
import numpy as np

from zvt.contract.api import df_to_db
from zvt.contract.recorder import Recorder, TimeSeriesDataRecorder
from zvt.utils.time_utils import now_pd_timestamp
from zvt.api.quote import china_stock_code_to_id
from zvt.domain import BlockStock, BlockCategory, Block
from zvt import init_log, zvt_env
from datetime import datetime, date


from jqdatasdk import auth, is_auth, get_query_count, get_industries, get_concepts, get_industry_stocks, \
    get_concept_stocks, get_all_trade_days


class JoinquantChinaBlockRecorder(Recorder):
    provider = 'joinquant'
    data_schema = Block

    def __init__(self):
        auth(zvt_env['jq_username'], zvt_env['jq_password'])
        is_authed = is_auth()
        print(is_authed)
        print(get_query_count())

    def convert_entity_id(self, x):
        # x.name == index
        # x['name']
        # x['start_date']
        return "block_cn_{0}".format(x.name)

    # 用于抓取行业/概念/地域列表
    def run(self):
        industries = ['sw_l1', 'sw_l2', 'sw_l3', 'zjw', 'jq_l1', 'jq_l2', 'concept']
        for category in industries:
            if category != 'concept':
                df_orig = get_industries(category)
            else:
                df_orig = get_concepts()
            print(df_orig)

            df_target = pd.DataFrame()
            df_target['id'] = df_orig.apply(lambda x: self.convert_entity_id(x), axis=1)
            df_target['entity_id'] = df_target['id']
            df_target['entity_type'] = 'block'
            df_target['exchange'] = 'cn'
            df_target['code'] = df_orig.apply(lambda x: x.name, axis=1)
            df_target['name'] = df_orig.apply(lambda x: x['name'], axis=1)
            df_target['category'] = category
            df_target['list_date'] = df_orig.apply(lambda x: x['start_date'], axis=1)
            print(df_target)

            if len(df_target) > 0:
                df_to_db(data_schema=self.data_schema, df=df_target, provider=self.provider,
                         force_update=True)

            self.logger.info(f"finish record sina blocks:{category}")


def convert_stock_code(jq_stock: str):
    return jq_stock.split(".")[0]


def convert_stock_entity_id(jq_stock: str):
    code = jq_stock.split(".")[0]
    exchange = jq_stock.split(".")[1]
    if exchange == 'XSHE':
        return f"stock_sz_{code}"
    elif exchange == 'XSHG':
        return f"stock_sh_{code}"
    else:
        raise ValueError("Invalid format: {0}".format(exchange))


def get_record_date(start_date: date, next_i: int, all_trade_days: list):
    """
    由于数据源的原因，start_date可能小于all_trade_days
    """
    if start_date < all_trade_days[0]:
        return all_trade_days[0]
    elif start_date > all_trade_days[-1]:
        return all_trade_days[-1]

    index = 0
    count = len(all_trade_days)
    for day in all_trade_days:
        if day == start_date:
            break
        index += 1

    if index + next_i < count:
        result = all_trade_days[index + next_i]
        if result > datetime.now().date():
            return None
        else:
            return result
    else:
        return None


class JoinquantChinaBlockStockRecorder(TimeSeriesDataRecorder):
    entity_provider = 'joinquant'
    entity_schema = Block

    provider = 'joinquant'
    data_schema = BlockStock

    # 用于抓取行业包含的股票
    def __init__(self, entity_type='block', exchanges=None, entity_ids=None, codes=None, batch_size=10,
                 force_update=True, sleeping_time=5, default_size=2000, real_time=False, fix_duplicate_way='add',
                 start_timestamp=None, end_timestamp=None, close_hour=0, close_minute=0) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute)
        auth(zvt_env['jq_username'], zvt_env['jq_password'])
        is_authed = is_auth()
        self.logger.info(f'is_authed: {is_authed}')
        self.logger.info('Query count: {0}'.format(get_query_count()))
        self.all_trade_days = get_all_trade_days().tolist()

    def record(self, entity, start, end, size, timestamps):
        if start is None:
            start_date = entity.list_date.date()
        else:
            start_date = start.date()

        date_type = type(start_date)

        # 循环存储
        for i in range(size):
            next_date = get_record_date(start_date, i, self.all_trade_days)
            if next_date is None:
                continue

            self.logger.info('Record {0} at {1}'.format(entity.code, next_date.strftime("%Y-%m-%d")))
            if entity.category != 'concept':
                stock_list = get_industry_stocks(entity.code, next_date)
            else:
                stock_list = get_concept_stocks(entity.code, next_date)

            the_list = []
            for stock in stock_list:
                stock_code = convert_stock_code(stock)
                stock_id = convert_stock_entity_id(stock)
                block_id = entity.id
                the_list.append({
                    'id': '{}_{}'.format(block_id, stock_id),
                    'entity_id': block_id,
                    'entity_type': 'block',
                    'exchange': entity.exchange,
                    'code': entity.code,
                    'name': entity.name,
                    'timestamp': pd.Timestamp(next_date),
                    'stock_id': stock_id,
                    'stock_code': stock_code,
                    'stock_name': '',
                })

            if the_list:
                df = pd.DataFrame.from_records(the_list)
                df_to_db(data_schema=self.data_schema, df=df, provider=self.provider,
                         force_update=True)

        self.logger.info('finish recording BlockStock:{},{}'.format(entity.category, entity.name))


__all__ = ['JoinquantChinaBlockRecorder', 'JoinquantChinaBlockStockRecorder']


if __name__ == '__main__':
    # init_log('sina_china_stock_category.log')

    # recorder = JoinquantChinaBlockRecorder()
    # recorder.run()

    recorder = JoinquantChinaBlockStockRecorder()
    recorder.run()
