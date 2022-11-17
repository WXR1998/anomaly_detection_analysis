import copy
import json

from sam.base import messageAgent as ma

from netio import protocol


def empty_anomaly_report_set():
    data = {
        ma.TURBONET_ZONE: {
            protocol.ATTR_FAILURE: {
                protocol.ATTR_SWITCH_ID_LIST: set(),
                protocol.ATTR_SERVER_ID_LIST: set(),
                protocol.ATTR_LINK_ID_LIST: set()
            },
            protocol.ATTR_ABNORMAL: {
                protocol.ATTR_SWITCH_ID_LIST: set(),
                protocol.ATTR_SERVER_ID_LIST: set(),
                protocol.ATTR_LINK_ID_LIST: set()
            },
        },
        ma.SIMULATOR_ZONE: {
            protocol.ATTR_FAILURE: {
                protocol.ATTR_SWITCH_ID_LIST: set(),
                protocol.ATTR_SERVER_ID_LIST: set(),
                protocol.ATTR_LINK_ID_LIST: set()
            },
            protocol.ATTR_ABNORMAL: {
                protocol.ATTR_SWITCH_ID_LIST: set(),
                protocol.ATTR_SERVER_ID_LIST: set(),
                protocol.ATTR_LINK_ID_LIST: set()
            },
        }
    }
    return copy.deepcopy(data)

def get_anomaly_report_list(data: dict) -> dict:
    return {
        protocol.ATTR_ALL_ZONE_DETECTION_DICT: {
            zone: {
                anomaly_type: {
                    id_list_key: list(id_list)
                    for id_list_key, id_list in anomaly_desc.items()
                } for anomaly_type, anomaly_desc in zone_desc.items()
            } for zone, zone_desc in data.items()
        }
    }

def data_format(d: object, limit: int=10) -> str:
    """
    将dict等格式化输出，较长的list只显示前n项
    """
    result = None

    if type(d) == dict:
        for k, v in d.items():
            r = f'{k}: {data_format(v)}'
            if result is None:
                result = r
            else:
                result = result + ', ' + r
        result = '{' + result + '}'
    elif type(d) in (list, tuple):
        l = len(d)
        if len(d) > limit:
            d = d[:limit]
        result = f'{json.dumps(d)}' + ('' if l <= limit else f' (len: {l})')
    else:
        result = str(type(d))

    return result
