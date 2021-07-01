# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base

from zvt.contract import Mixin
from zvt.contract.register import register_schema

OverallBase = declarative_base()


# 市场整体估值

class StockSummary(OverallBase, Mixin):
    __tablename__ = 'stock_summary'

    provider = Column(String(length=32))
    code = Column(String(length=32))
    name = Column(String(length=32))

    total_value = Column(Float)
    total_tradable_vaule = Column(Float)
    pe = Column(Float)
    pb = Column(Float)
    volume = Column(Float)
    turnover = Column(Float)
    turnover_rate = Column(Float)


# 融资融券概况

class MarginTradingSummary(OverallBase, Mixin):
    __tablename__ = 'margin_trading_summary'
    provider = Column(String(length=32))
    code = Column(String(length=32))
    name = Column(String(length=32))

    # 融资余额
    margin_value = Column(Float)
    # 买入额
    margin_buy = Column(Float)
    # 融资偿还额
    margin_return = Column(Float)

    # 融券余额
    short_value = Column(Float)
    # 卖出量, 股,份,手
    short_volume = Column(Float)

    # 融资融券余额，元
    total_value = Column(Float)
    # 融券余量(股,份,手)
    short_total_value = Column(Float)


# 沪股通/深股通，沪港通/深港通
class HSGCrossMarketSummary(OverallBase, Mixin):
    __tablename__ = 'hsg_cross_market_summary'
    provider = Column(String(length=32))
    code = Column(String(length=32))
    name = Column(String(length=32))

    # 港股通，上海（元）
    ggt_ss_amount = Column(Float)
    # 港股通，深圳（元）
    ggt_sz_amount = Column(Float)
    # 沪港通（元）
    hgt_amount = Column(Float)
    # 深港通（元）
    sgt_amount = Column(Float)
    # 北向资金（元）
    north_total_amount = Column(Float)
    # 南向资金（元）
    south_total_amount = Column(Float)


# 北向/南向成交概况
class CrossMarketSummary(OverallBase, Mixin):
    __tablename__ = 'cross_market_summary'
    provider = Column(String(length=32))
    code = Column(String(length=32))
    name = Column(String(length=32))

    buy_amount = Column(Float)
    buy_volume = Column(Float)
    sell_amount = Column(Float)
    sell_volume = Column(Float)
    quota_daily = Column(Float)
    quota_daily_balance = Column(Float)


# 港股通持股明细
class HKHold(OverallBase, Mixin):
    __tablename__ = 'hk_hold'
    code = Column(String(length=32))
    name = Column(String(length=32))
    # 原始代码
    original_code = Column(String(length=32))
    # 持股数量
    hold_volume = Column(Integer)
    # 持股比例
    hold_ratio = Column(Float)
    # 类型: SH沪股通SZ深股通HK港股通
    exchange = Column(String(length=32))


# 基金申购赎回情况
class FundTradeInfo(OverallBase, Mixin):
    __tablename__ = 'fund_trade_info'
    provider = Column(String(length=32))
    code = Column(String(length=32))
    name = Column(String(length=32))

    fund_code = Column(String(length=32))
    fund_name = Column(String(length=32))
    buy_status = Column(String(length=32))
    buy_limit = Column(Float)
    sell_status = Column(String(length=32))


register_schema(providers=['ts', 'joinquant', 'exchange', 'eastmoney'], db_name='overall', schema_base=OverallBase)

__all__ = ['StockSummary', 'MarginTradingSummary', 'CrossMarketSummary', 'HSGCrossMarketSummary', 'FundTradeInfo', 'HKHold']
