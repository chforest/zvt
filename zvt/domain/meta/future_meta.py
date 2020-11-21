# -*- coding: utf-8 -*-

from sqlalchemy import Column, String, DateTime, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base

from zvt.contract import EntityMixin
from zvt.contract.register import register_schema, register_entity
from zvt.utils.time_utils import now_pd_timestamp

FutureMetaBase = declarative_base()


class BaseSecurity(EntityMixin):
    # 上市日
    list_date = Column(DateTime)
    # 退市日
    end_date = Column(DateTime)


# 个股
@register_entity(entity_type='future')
class Future(FutureMetaBase, BaseSecurity):
    __tablename__ = 'future'

    # 对应指数
    index = Column(String(length=64))


register_schema(providers=['investing'], db_name='future_meta',
                schema_base=FutureMetaBase)

__all__ = ['Future']
