import logging

import requests

from zvt.contract.api import get_data_count, get_data
from zvt.contract.recorder import TimestampsDataRecorder, TimeSeriesDataRecorder
from zvt.utils.time_utils import to_pd_timestamp
from zvt.domain import CompanyType, Stock, StockDetail

logger = logging.getLogger(__name__)


def get_from_path_fields(the_json, path_fields):
    the_data = the_json.get(path_fields[0])
    if the_data:
        for field in path_fields[1:]:
            the_data = the_data.get(field)
            if not the_data:
                return None
    return the_data


def call_hexun_api(url=None, method='post', param=None, path_fields=None):
    if method == 'post':
        resp = requests.post(url, json=param)

    resp.encoding = 'utf8'

    try:
        origin_result = resp.json().get('Result')
    except Exception as e:
        logger.exception('code:{},content:{}'.format(resp.status_code, resp.text))
        raise e

    if path_fields:
        the_data = get_from_path_fields(origin_result, path_fields)
        if not the_data:
            logger.warning(
                "url:{},param:{},origin_result:{},could not get data for nested_fields:{}".format(url, param,
                                                                                                  origin_result,
                                                                                                  path_fields))
        return the_data

    return origin_result



class ApiWrapper(object):
    def request(self, url=None, method='post', param=None, path_fields=None):
        raise NotImplementedError


class HexunApiWrapper(ApiWrapper):
    def request(self, url=None, method='post', param=None, path_fields=None):
        return call_hexun_api(url=url, method=method, param=param, path_fields=path_fields)


class BaseHexunRecorder(object):
    request_method = 'post'
    path_fields = None
    api_wrapper = HexunApiWrapper()

    def generate_request_param(self, security_item, start, end, size, timestamp):
        raise NotImplementedError

    def record(self, entity_item, start, end, size, timestamps):
        if timestamps:
            original_list = []
            for the_timestamp in timestamps:
                param = self.generate_request_param(entity_item, start, end, size, the_timestamp)
                tmp_list = self.api_wrapper.request(url=self.url, param=param, method=self.request_method,
                                                    path_fields=self.path_fields)
                self.logger.info(
                    "record {} for entity_id:{},timestamp:{}".format(
                        self.data_schema, entity_item.id, the_timestamp))
                # fill timestamp field
                for tmp in tmp_list:
                    tmp[self.get_evaluated_time_field()] = the_timestamp
                original_list += tmp_list
                if len(original_list) == self.batch_size:
                    break
            return original_list

        else:
            param = self.generate_request_param(entity_item, start, end, size, None)
            return self.api_wrapper.request(url=self.url, param=param, method=self.request_method,
                                            path_fields=self.path_fields)


class HexunTimestampsDataRecorder(BaseHexunRecorder, TimestampsDataRecorder):
    entity_provider = 'joinquant'
    entity_schema = StockDetail

    provider = 'hexun'

    timestamps_fetching_url = None
    timestamp_list_path_fields = None
    timestamp_path_fields = None

    def init_timestamps(self, entity):
        param = {
            "color": "w",
        }

        # 根据entity的list_date计算开始timestamp
        list_date = entity.list_date

        timestamp_json_list = call_hexun_api(url=self.timestamps_fetching_url,
                                                 path_fields=self.timestamp_list_path_fields,
                                                 param=param)

        if self.timestamp_path_fields and timestamp_json_list:
            timestamps = [get_from_path_fields(data, self.timestamp_path_fields) for data in timestamp_json_list]
            return [to_pd_timestamp(t) for t in timestamps]
        return []

