# -*- coding: utf-8 -*-
from zvt.contract import IntervalLevel
from zvt.domain import ReportPeriod
from zvt.api import AdjustType
from datetime import datetime, timedelta


def get_coarse_end_date(start:datetime, days: int) -> datetime:
    trade_days = days*7/5
    end_date = start + timedelta(trade_days)
    if end_date > datetime.now():
        end_date = datetime.now()
    return end_date


def to_ts_trading_level(trading_level: IntervalLevel):
    if trading_level < IntervalLevel.LEVEL_1HOUR:
        return '{0}in'.format(trading_level.value)
    if trading_level == IntervalLevel.LEVEL_1HOUR:
        return '60min'
    if trading_level == IntervalLevel.LEVEL_4HOUR:
        return '240min'
    if trading_level == IntervalLevel.LEVEL_1DAY:
        return 'D'
    if trading_level == IntervalLevel.LEVEL_1WEEK:
        return 'W'
    if trading_level == IntervalLevel.LEVEL_1MON:
        return 'M'


def to_ts_date(date_: datetime):
    return date_.strftime("%Y%m%d")


def to_ts_adj(adjust_type):
    if adjust_type == AdjustType.hfq:
        return 'hfq'
    elif adjust_type == AdjustType.qfq:
        return 'qfq'
    else:
        return None


def to_ts_entity_id(security_item):
    if security_item.entity_type == 'stock' or security_item.entity_type == 'etf':
        if security_item.exchange == 'sh':
            return '{}.SH'.format(security_item.code)
        if security_item.exchange == 'sz':
            return '{}.SZ'.format(security_item.code)
    elif security_item.entity_type == 'index':
        if security_item.code[0] == '0':
            return '{}.SH'.format(security_item.code)
        elif security_item.code[0] == '3':
            return '{}.SZ'.format(security_item.code)
        elif security_item.code[0] == '9':
            return '{}.CSI'.format(security_item.code)
        else:
            raise ValueError("Not support!")
    else:
        raise ValueError("Not support!")


def to_entity_id(jq_code: str, entity_type):
    code, exchange = jq_code.split('.')
    if exchange == 'XSHG':
        exchange = 'sh'
    elif exchange == 'XSHE':
        exchange = 'sz'

    return f'{entity_type}_{exchange}_{code}'
